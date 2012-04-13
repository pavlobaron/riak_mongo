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

-module(riak_mongo_store).

-include("riak_mongo_bson2.hrl").
-include("riak_mongo_protocol.hrl").
-include("riak_mongo_state.hrl").

-export([insert/2, find/1]).

insert(#mongo_insert{dbcoll=Collection, documents=Docs, continueonerror=ContinueOnError}, State) ->

    {ok, C} = riak:local_client(),

    Errors =
        lists:foldl(fun(#bson_raw_document{ id=BSON_ID, body=Doc }, Err)
                          when Err=:=[]; ContinueOnError=:=true ->
                            ID = bson_to_riak_key(BSON_ID),

                            O = riak_object:new(Collection, ID, Doc, "application/bson"),

                            error_logger:info_msg("storing ~p~n", [O]),

                            case C:put(O) of
                                ok -> Err;
                                Error -> [Error|Err]
                            end
                    end,
                    [],
                    Docs),

    State#state{ lastError=Errors }.


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


find(#mongo_query{dbcoll=Collection, selector=Selector, projector=Projection }) ->

    Project = compute_projection_fun(Projection),
    CompiledQuery = riak_mongo_query:compile(Selector),

    {ok, Documents}
        = riak_kv_mrc_pipe:mapred(Collection,
                                  [{map, {qfun, fun map_query/3}, {CompiledQuery, Project}, true}]),

    Documents.


map_query(Object, _KeyData, {CompiledQuery, Project}) ->
    Acc = [],
    MD = riak_object:get_metadata(Object),
    case dict:find(<<"content-type">>, MD) of
        {ok, "application/bson"} ->
            BSON = riak_object:get_value(Object),

            case catch riak_mongo_bson2:get_document(BSON) of
                {{struct, _}=Document, _} ->
                    do_mongo_match(Document, CompiledQuery, Project, Acc);
                _ ->
                    Acc
            end;

        %% also query any JSON documents
        {ok, "application/json"} ->
            JSON = riak_object:get_value(Object),

            case catch mochijson2:decode(JSON) of
                {struct, _}=Document ->
                    do_mongo_match(Document, CompiledQuery, Project, Acc);
                _ ->
                    Acc
            end;

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



delete(Delete) ->
    error_logger:error_msg("delete not implemented: ~p~n", Delete),
    ok.


bson_to_riak_key({objectid, BIN}) ->
    iolist_to_binary("OID:" ++ hexencode(BIN)).

hexencode(<<>>) -> [];
hexencode(<<CH, Rest/binary>>) ->
    [ hex(CH) | hexencode(Rest) ].

hex(CH) when CH < 16 ->
    [ $0, integer_to_list(CH, 16) ];
hex(CH) ->
    integer_to_list(CH, 16).


