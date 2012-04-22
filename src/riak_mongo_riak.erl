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

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include("riak_mongo_bson.hrl").
-include("riak_mongo_protocol.hrl").
-include("riak_mongo_state.hrl").

-compile([{parse_transform, lager_transform}]).

-export([insert/2, find/2, getmore/2, delete/2, update/2]).

-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_FIND_SIZE, 101).

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


getmore(#mongo_getmore{ cursorid=CursorID, batchsize=BatchSize }, #worker_state{ cursors=Dict } = State) ->

    case dict:find(CursorID, Dict) of
        {ok, {_, CursorPID}} ->

            case cursor_get_results(CursorPID, BatchSize) of

                {more, StartingFrom, Documents} ->
                    {ok,
                     #mongo_reply{ startingfrom = StartingFrom,
                                   cursorid     = CursorID,
                                   documents    = Documents },
                     State};

                {done, StartingFrom, Documents} ->
                    {ok,
                     #mongo_reply{ startingfrom = StartingFrom,
                                   cursorid     = 0,
                                   documents    = Documents },
                     cursor_remove(CursorID, State) }
            end;

        error ->
            {ok, #mongo_reply{ cursornotfound=true, documents=[] }, State}
    end.

find(#mongo_query{dbcoll=Bucket, selector=Selector, projector=Projection, batchsize=BatchSize,
                 nocursortimeout=NoTimeout, tailablecursor=_Tailable }, State) ->

    Project = compute_projection_fun(Projection),

    case id_document(Selector) of

        {ok, RiakKey} ->
            {ok, C} = riak:local_client(),
            case C:get(Bucket, RiakKey) of
                {ok, RiakObject} ->
                    case riak_to_bson_object(RiakObject) of
                        {ok, Document} ->
                            Result = [Project(RiakObject, Document)],
                            find_reply(Result, State);
                        _ ->
                            find_reply([], State)
                    end;
                _ ->
                    find_reply([], State)
            end;

        false ->
            CompiledQuery = riak_mongo_query:compile(Selector),

            %% TODO: Server side does not know the LIMIT
            if
                BatchSize == 0 ->
                    Batch = ?DEFAULT_FIND_SIZE;
                true ->
                    Batch = abs(BatchSize)
            end,

            error_logger:info_msg("Find executed ~p, ~p, ~p~n", [Projection, CompiledQuery, Project]),

            Owner = self(),
            CursorPID =
                proc_lib:spawn(fun() ->
                                       cursor_init(Owner, Bucket, CompiledQuery, Project,
                                                   NoTimeout)
                               end),

            case cursor_get_results(CursorPID, Batch) of
                {more, StartingFrom, Documents} ->

                    if BatchSize < 0 ->
                            CursorPID ! die,
                            {ok,
                             #mongo_reply{ startingfrom = StartingFrom,
                                           documents    = Documents },
                             State};

                       true ->
                            {ok, CursorID, State2} = cursor_add(CursorPID, State),
                            {ok,
                             #mongo_reply{ startingfrom = StartingFrom,
                                           cursorid     = CursorID,
                                           documents    = Documents },
                             State2}
                    end;

                {done, StartingFrom, Documents} ->
                    {ok,
                     #mongo_reply{ startingfrom = StartingFrom,
                                   documents    = Documents },
                     State}
            end
    end.

update(#mongo_update{dbcoll=Bucket, selector=Selector, updater=Updater, rawupdater=RawUpdater,
		     multiupdate=MultiUpdate, upsert=Upsert}, State) ->

    error_logger:info_msg("About to update ~p, ~p, ~p~n", [Updater, MultiUpdate, Upsert]),

    case id_document(Selector) of
        {ok, RiakKey} when not Upsert ->
            {ok, C} = riak:local_client(),
            case C:get(Bucket, RiakKey) of
                {ok, RiakObject} ->
		    NewObject = riak_object:update_value(RiakObject, RawUpdater),
		    case C:put(NewObject) of
			ok -> State;
			Err -> State#worker_state{ lastError=Err }
                    end;
                Err -> State#worker_state{ lastError=Err }
            end;
	_ ->
	    error_logger:info_msg("This update variant is not yet supported~n", []),
	    State
    end.

delete(#mongo_delete{dbcoll=Bucket, selector=Selector, singleremove=SingleRemove}, State) ->

    case id_document(Selector) of
        {ok, RiakKey} ->
            {ok, C} = riak:local_client(),
            case C:delete(Bucket, RiakKey) of
                ok -> State;
                Err -> State#worker_state{ lastError=Err }
            end;

        false when not SingleRemove ->

            Project = fun(RiakObject, _) ->
                              {ok, C} = riak:local_client(),
                              case C:delete(Bucket, riak_object:key(RiakObject)) of
                                  ok -> ok;
                                  Err -> Err
                              end
                      end,

            CompiledQuery = riak_mongo_query:compile(Selector),

            {ok, Errors}
                = riak_kv_mrc_pipe:mapred(Bucket,
                                          [{map, {qfun, fun map_query/3}, {CompiledQuery, Project}, true}]),


            State#worker_state{ lastError=Errors };

        false when SingleRemove ->

            Project = fun(RiakObject, _) -> riak_object:key(RiakObject) end,
            CompiledQuery = riak_mongo_query:compile(Selector),

            case riak_kv_mrc_pipe:mapred(Bucket,
                                         [{map, {qfun, fun map_query/3}, {CompiledQuery, Project}, true}])
            of
                {ok, []} ->
                    State;

                {ok, [RiakKey|_]} ->
                    {ok, C} = riak:local_client(),
                    case C:delete(Bucket, RiakKey) of
                        ok -> State;
                        Err -> State#worker_state{ lastError=Err }
                    end
            end
    end.


%% internals

cursor_add(PID, #worker_state{ cursors=Dict, cursor_next=ID }=State) ->
    MRef = erlang:monitor(process, PID),
    {ok, ID, State#worker_state{ cursors=dict:store(ID,{MRef,PID},Dict), cursor_next=ID+1 }}.

cursor_remove(CursorID, #worker_state{ cursors=Dict }=State) ->
    {MRef,_PID} = dict:fetch(CursorID, Dict),
    erlang:demonitor(MRef, [flush]),
    State#worker_state{ cursors=dict:erase(CursorID, Dict) }.

cursor_init(Owner, Bucket, CompiledQuery, Project, NoTimeout) ->

    TimeOut = case NoTimeout of
                  true -> infinity;
                  false -> ?DEFAULT_TIMEOUT
              end,

    OwnerRef = erlang:monitor(process, Owner),

    {{ok, Pipe}, _} =
        riak_kv_mrc_pipe:mapred_stream([{map, {qfun, fun map_query/3}, {CompiledQuery, Project}, true}]),

    case riak_kv_mrc_pipe:send_inputs(Pipe, Bucket, TimeOut) of
        ok ->
            collect_outputs(OwnerRef, Pipe, TimeOut);
        Error ->
            lager:error("pipe:send_inputs faild ~p~n", [Error]),
            riak_pipe:eoi(Pipe),
            collect_outputs(OwnerRef, Pipe, TimeOut)
    end.


collect_outputs(OwnerRef, Pipe, Timeout) ->
    cursor_main_loop(OwnerRef, Pipe, queue:new(), Timeout, 0, 0, more).

%% TODO: check if it would make sense to have this processes under supervision (ETS for cursor?)
cursor_main_loop(OwnerRef, #pipe{sink=#fitting{ref=FittingRef}} = Pipe, ResultQueue, Timeout, Sent, N, State) ->

    receive
        #pipe_result{ref=FittingRef, result=Result} ->
            cursor_main_loop(OwnerRef, Pipe, queue:in(Result, ResultQueue), Timeout, Sent, N+1, State);
        #pipe_log{ref=FittingRef, msg=Msg} ->
            lager:info("riak_mongo: ~s~n", [Msg]),
            cursor_main_loop(OwnerRef, Pipe, ResultQueue, Timeout, Sent, N, State);
        #pipe_eoi{ref=FittingRef} ->
            cursor_main_loop(OwnerRef, Pipe, ResultQueue, Timeout, Sent, N, done);

        {'DOWN', OwnerRef, _, _, _} ->
            %% worker died
            riak_pipe:destroy(Pipe),
            ok;

        die ->
            riak_pipe:destroy(Pipe),
            ok;

        {next, {PID, ReplyRef}, NUM} when N >= NUM ->
            {Q1,Q2} = queue:split(min(NUM,N), ResultQueue),
            case State of
                more ->
                    PID ! {more, ReplyRef, Sent, queue:to_list(Q1)},
                    cursor_main_loop(OwnerRef, Pipe, Q2, Timeout, Sent + NUM, N-NUM, done);
                done ->
                    PID ! {done, ReplyRef, Sent, queue:to_list(Q1)},
                    ok
            end;

        {next, {PID, ReplyRef}, _} when State =:= done ->
            PID ! {done, ReplyRef, Sent, queue:to_list(ResultQueue)},
            ok;

        MSG when tuple_size(MSG) =/= 3, element(1,MSG) =/= next ->
            error_logger:info_msg("cursor_main_loop.6 ~p~n", [MSG]),
            ok


    after Timeout ->
            cursor_main_loop(OwnerRef, Pipe, ResultQueue, infinity, Sent, N, done)

    end.

cursor_get_results(CursorPID, HowMany) ->
    Ref = erlang:monitor(process, CursorPID),
    CursorPID ! {next, {self(), Ref}, HowMany},
    receive
        {more, Ref, StartingFrom, Documents} ->
            erlang:demonitor(Ref, [flush]),
            {more, StartingFrom, Documents};
        {done, Ref, StartingFrom, Documents} ->
            erlang:demonitor(Ref, [flush]),
            {done, StartingFrom, Documents};
        {'DOWN', Ref, _, _, Reason} ->
            {error, Reason}
    end.

find_reply(Documents,State) ->
    {ok, #mongo_reply{ documents=Documents, queryerror=false  }, State}.

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

map_query(RiakObject, _KeyData, {CompiledQuery, Project}) ->
    Acc = [],
    case riak_to_bson_object(RiakObject) of
        {ok, Document} ->
            do_mongo_match(RiakObject, Document, CompiledQuery, Project, Acc);
        _ ->
            Acc
    end.

do_mongo_match(RiakObject,Document,CompiledQuery,Project,Acc) ->
    case riak_mongo_query:matches(Document,
                                  CompiledQuery) of
        true ->
            [Project(RiakObject, Document)|Acc];
        false ->
            Acc
    end.

compute_projection_fun(Projection) ->
    case Projection of
        [] ->
            fun(_RiakObject, O) -> O end;

        List when is_list(List) ->
            SelectedKeys = get_projection_keys(Projection, []),
            fun(_RiakObject, {struct, Elems}) ->
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
