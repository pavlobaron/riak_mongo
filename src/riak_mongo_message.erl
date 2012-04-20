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

%% @author Kresten Krab Thorup <krab@trifork.com>
%% @author Pavlo Baron <pb at pbit dot org>
%% @doc Here we process all kind of messages
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_message).

-export([process_messages/2]).

-include ("riak_mongo_protocol.hrl").
-include("riak_mongo_state.hrl").
-include_lib("bson/include/bson_binary.hrl").

-define(CMD,<<"$cmd">>).
-define(ADM,<<"admin">>).

%%
%% loop over messages
%%
process_messages([], State) ->
    State;
process_messages([Message|Rest], State) ->
    error_logger:info_msg("processing ~p~n", [Message]),
    case process_message(Message, State) of
        {noreply, OutState} ->
            ok;

        {reply, Reply, #worker_state{sock=Sock}=State2} ->
            MessageID = element(2, Message),
            ReplyMessage = Reply#mongo_reply{ request_id = State2#worker_state.request_id,
                                              reply_to = MessageID },
            OutState = State2#worker_state{ request_id = (State2#worker_state.request_id+1) },

            error_logger:info_msg("replying ~p~n", [ReplyMessage]),

            {ok, Packet} = riak_mongo_protocol:encode_packet(ReplyMessage),
            Size = byte_size(Packet),
            gen_tcp:send(Sock, <<?put_int32(Size+4), Packet/binary>>)
    end,

    process_messages(Rest, OutState);
process_messages(A1,A2) ->
    error_logger:info_msg("BAD ~p,~p~n", [A1,A2]),
    exit({badarg,A1,A2}).

process_message(#mongo_query{ db=DataBase, coll=?CMD,
                              selector=Selector}, State) ->

    {struct, [{Command,_}|Options]} = Selector,

    case db_command(DataBase, Command, Options, State) of
        {ok, Reply, State2} ->
            {reply, #mongo_reply{ documents=[ {struct, Reply} ]} , State2}
    end
;

process_message(#mongo_query{}=Message, State) ->

    Result = riak_mongo_riak:find(Message),
    {reply, #mongo_reply{ documents=Result, queryerror=false }, State};

process_message(#mongo_insert{}=Insert, State) ->
    State2 = riak_mongo_riak:insert(Insert, State),
    {noreply, State2};

process_message(#mongo_delete{}=Delete, State) ->
    State2 = riak_mongo_riak:delete(Delete, State),
    {noreply, State2};

process_message(Message, State) ->
    error_logger:info_msg("unhandled message: ~p~n", [Message]),
    {noreply, State}.

%% internals
you(#worker_state{sock=Sock}) ->
    {ok, {{A, B, C, D}, P}} = inet:peername(Sock), %IPv6???
    io_lib:format("~p.~p.~p.~p:~p", [A, B, C, D, P]).

db_command(?ADM, <<"whatsmyuri">>, _Options, State) ->
    {ok, [{you, {utf8, you(State)}}, {ok, 1}], State};

db_command(?ADM, <<"replSetGetStatus">>, _Options, State) ->
    _IsForShell = proplists:is_defined(forShell, _Options),
    {ok, [{ok, false}], State};

db_command(_DataBase, <<"getlasterror">>, _Options, State) ->
    case State#worker_state.lastError of
        [] ->
            {ok, [{ok,true}], State#worker_state{lastError=[]}};
        MSG ->
            {ok, [{err, io:format("~p", MSG)}], State#worker_state{lastError=[]}}
    end;

db_command(DataBase, Command, _Options, State) ->
    {ok, [{err, <<"unknown command: db=", DataBase, ", cmd=", Command/binary>>}, {ok, false}], State}.
