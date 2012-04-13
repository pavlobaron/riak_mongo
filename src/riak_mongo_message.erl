%%
%% This file is part of riak_mongo
%%
%% Copyright (c) 2012 by Pavlo Baron (pb at pbit dot org)
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
%% @doc Here we process all kind of messages
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_message).

-export([process_message/2]).

-include ("riak_mongo_protocol.hrl").
-include_lib("riak_mongo_state.hrl").

process_message(#mongo_query{ dbcoll= <<"admin.$cmd">>,
                              selector=Selector }, State) ->

    {struct, [{Command,1}|Options]} = Selector,

    case admin_command(Command, Options, State) of
        {ok, Reply, State2} ->
            {reply, #mongo_reply{ documents=[ {struct, Reply} ]} , State2}
    end
;

process_message(#mongo_query{ dbcoll= <<"collection.$cmd">>,
                              selector=Selector}, State) ->

    {struct, [{Command,_}|Options]} = Selector,

    case collection_command(Command, Options, State) of
        {ok, Reply, State2} ->
            {reply, #mongo_reply{ documents=[ {struct, Reply} ]} , State2}
    end
;

process_message(#mongo_query{}=Message, State) ->

    error_logger:info_msg("query: ~p~n", [Message]),

    Result = riak_mongo_store:find(Message),
    {reply, #mongo_reply{ documents=Result, queryerror=false }, State};

process_message(#mongo_insert{}=Insert, State) ->
    State2 = riak_mongo_store:insert(Insert, State),
    {noreply, State2};

process_message(Message, State) ->
    error_logger:info_msg("unhandled message: ~p~n", [Message]),
    {noreply, State}.

%% internals
you(#state{peer=Peer}) ->
    {ok, {{A, B, C, D}, P}} = Peer, %IPv6???
    io_lib:format("~p.~p.~p.~p:~p", [A, B, C, D, P]).


admin_command(<<"whatsmyuri">>, _Options, State) ->
    {ok, [{you, {utf8, you(State)}}, {ok, 1}], State};

admin_command(<<"replSetGetStatus">>, _Options, State) ->
    _IsForShell = proplists:is_defined(forShell, _Options),
    {ok, [{ok, false}], State};

admin_command(Command, _Options, State) ->
    {ok, [{err, <<"unknown command: ", Command/binary>>}, {ok, false}], State}.

collection_command(<<"getlasterror">>, _Options, State) ->
    case State#state.lastError of
        [] ->
            {ok, [{ok,true}], State#state{lastError=[]}};
        MSG ->
            {ok, [{err, io:format("~p", MSG)}], State#state{lastError=[]}}
    end;


collection_command(Command, _Options, State) ->
    {ok, [{err, <<"unknown command: ", Command/binary>>}, {ok, false}], State}.

