%%
%% This file is part of riak_mongo
%%
%% Copyright (c) 2012 by Pavlo Baron (pb at pbit dot org)
%% Copyright (c) 2012 by Trifork
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

%% @author Pavlo Baron <pb at pbit dot org>
%% @author Kresten Krab Thorup <krab@trifork.com>
%% @doc Here we speak to the Riak store
%% @copyright 2012 Pavlo Baron and Trifork

-module(riak_mongo_riak).

-include("riak_mongo_bson.hrl").
-include("riak_mongo_protocol.hrl").
-include("riak_mongo_state.hrl").

-export([insert/2, find/1, delete/2]).

insert(#mongo_insert{dbcoll=Bucket, documents=Docs, continueonerror=ContinueOnError}, State) ->

    {ok, C} = riak:local_client(),

    Errors =
        lists:foldl(fun(#bson_raw_document{ id=BSON_ID, body=Doc }, Err)
                          when Err=:=[]; ContinueOnError=:=true ->
                            ID = bson_to_riak_key(BSON_ID),

                            O = riak_object:new(Bucket, ID, Doc, "application/bson"),

                            error_logger:info_msg("storing ~p~n", [O]),

                            case C:put(O) of
                                ok -> Err;
                                Error -> [Error|Err]
                            end
                    end,
                    [],
                    Docs),

    State#worker_state{ lastError=Errors }.

find(#mongo_query{dbcoll=Bucket, selector=Selector, projector=Projection, batchsize=BatchSize }) ->

    Project = compute_projection_fun(Projection),

    case id_document(Selector) of

        {ok, RiakKey} ->
            {ok, C} = riak:local_client(),
            case C:get(Bucket, RiakKey) of
                {ok, RiakObject} ->
                    case riak_to_bson_object(RiakObject) of
                        {ok, Document} -> [Project(Document)];
                        _ -> []
                    end;
                _ -> []
            end;

        false ->

            CompiledQuery = riak_mongo_query:compile(Selector),

            error_logger:info_msg("Find executed ~p, ~p, ~p~n", [Projection, CompiledQuery, Project]),

            %% TODO: simple key-based read shouldn't go through mapred for speed
            {ok, Documents}
                = riak_kv_mrc_pipe:mapred(Bucket,
                                          [{map, {qfun, fun map_query/3}, {CompiledQuery, Project}, true}]),

            %% TODO: dig deeper here to find out if it's possible to limit the
            %% number of returned docs during mapred, not afterwards.
            %% TODO2: Find a way to handle cursors ... the elements removed by "limit"
            %% should be held by a cursor
            case BatchSize /= 0 of
                true ->
                    error_logger:info_msg("Limiting result set to ~p docs~n", [BatchSize]),
                    limit_docs(Documents, abs(BatchSize), 0);
                false -> Documents
            end
    end.


id_document({struct, [{<<"_id">>, ID}]}) ->
    case
        case ID of
            {objectid, _} -> true;
            {binary, _} -> true;
            {md5, _} -> true;
            {uuid, _} -> true;
            _ when is_binary(ID) -> true;
            _ -> false
        end
    of
        true -> {ok, bson_to_riak_key(ID)};
        false -> false
    end;
id_document(_) ->
    false.

delete(#mongo_delete{dbcoll=Bucket, selector=Selector, singleremove=SingleRemove}, State) ->

    case id_document(Selector) of
        {ok, RiakKey} ->
            {ok, C} = riak:local_client(),
            case C:delete(Bucket, RiakKey) of
                ok -> State;
                Err -> State#worker_state{ lastError=Err }
            end;

        false when not SingleRemove ->

            Project = fun({struct, Elms}) ->
                              {<<"_id">>, ID} = lists:keyfind(<<"_id">>, 1, Elms),
                              {ok, C} = riak:local_client(),
                              case C:delete(Bucket, bson_to_riak_key(ID)) of
                                  ok -> [];
                                  Err -> [Err]
                              end
                      end,

            CompiledQuery = riak_mongo_query:compile(Selector),

            {ok, Errors}
                = riak_kv_mrc_pipe:mapred(Bucket,
                                          [{map, {qfun, fun map_query/3}, {CompiledQuery, Project}, true}]),


            State#worker_state{ lastError=Errors };

        false when SingleRemove ->

            Project = fun(Doc) -> Doc end,
            CompiledQuery = riak_mongo_query:compile(Selector),

            case riak_kv_mrc_pipe:mapred(Bucket,
                                         [{map, {qfun, fun map_query/3}, {CompiledQuery, Project}, true}])
            of
                {ok, []} ->
                    State;

                {ok, [{struct, Elms}|_]} ->
                    {<<"_id">>, ID} = lists:keyfind(<<"_id">>, 1, Elms),
                    {ok, C} = riak:local_client(),
                    case C:delete(Bucket, bson_to_riak_key(ID)) of
                        ok -> State;
                        Err -> State#worker_state{ lastError=Err }
                    end
            end
    end.


%% internals

limit_docs(_, BatchSize, N) when N =:= BatchSize ->
    [];
limit_docs([], _, _) ->
    [];
limit_docs([Document|T], BatchSize, N) ->
    [Document|limit_docs(T, BatchSize, N + 1)].

riak_to_bson_object(Object) ->
    MD = riak_object:get_metadata(Object),
    case dict:find(<<"content-type">>, MD) of
        {ok, "application/bson"} ->
            BSON = riak_object:get_value(Object),

            case catch riak_mongo_bson:get_document(BSON) of
                {{struct, _}=Document, _} ->
                    {ok, Document};
                _ ->
                    none
            end;

        %% also query any JSON documents
        {ok, "application/json"} ->
            JSON = riak_object:get_value(Object),

            case catch mochijson2:decode(JSON) of
                {struct, _}=Document ->
                    {ok, Document};
                _ ->
                    none
            end;

        _ ->
            none
    end.

map_query(Object, _KeyData, {CompiledQuery, Project}) ->
    Acc = [],
    case riak_to_bson_object(Object) of
        {ok, Document} ->
            do_mongo_match(Document, CompiledQuery, Project, Acc);
        _ ->
            Acc
    end.

do_mongo_match(Document,CompiledQuery,Project,Acc) ->
    case riak_mongo_query:matches(Document,
                                  CompiledQuery) of
        true ->
            [Project(Document)|Acc];
        false ->
            Acc
    end.

compute_projection_fun(Projection) ->
    case Projection of
        [] ->
            fun(O) -> O end;

        List when is_list(List) ->
            SelectedKeys = get_projection_keys(Projection, []),
            fun({struct, Elems}) ->
                    {struct,
                     lists:foldl(fun(Key,Acc) ->
                                         case lists:keyfind(Key, 1, Elems) of
                                             false ->
                                                 Acc;
                                             KV ->
                                                 [KV|Acc]
                                         end
                                 end,
                                 [],
                                 SelectedKeys
                                )}
            end
    end.

get_projection_keys([], Acc) ->
    Acc;
get_projection_keys([{struct, KVs}|Rest], Acc) ->
    Keys = lists:usort
             (lists:foldl(fun({K,V}, Acc1) ->
                                  case is_true(V) of
                                      true -> [K|Acc1];
                                      false -> Acc1
                                  end
                          end,
                          [],
                          KVs)),

    get_projection_keys(Rest, lists:umerge(Keys,Acc)).

bson_to_riak_key({objectid, BIN}) ->
    iolist_to_binary("OID:" ++ hexencode(BIN));
bson_to_riak_key({binary, BIN}) ->
    iolist_to_binary("BIN:" ++ hexencode(BIN));
bson_to_riak_key({uuid, BIN}) ->
    iolist_to_binary("UUID:" ++ hexencode(BIN));
bson_to_riak_key({md5, BIN}) ->
    iolist_to_binary("MD5:" ++ hexencode(BIN));
bson_to_riak_key(BIN) when is_binary(BIN) ->
    BIN.

hexencode(<<>>) -> [];
hexencode(<<CH, Rest/binary>>) ->
    [ hex(CH) | hexencode(Rest) ].

hex(CH) when CH < 16 ->
    [ $0, integer_to_list(CH, 16) ];
hex(CH) ->
    integer_to_list(CH, 16).

%%
%% we'll need this lots of places, it should probably be
%% in a separate "js emulation" module
%%
is_true(undefined) -> false;
is_true(null) -> false;
is_true(false) -> false;
is_true(0) -> false;
is_true(0.0) -> false;
is_true(_) -> true.
