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

%% @author Pavlo Baron <pbat pbit dot org>
%% @doc This is the TCP server of riak_mongo
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_server).

-export([start_link/0]).

-export([init/2, handle_call/3, handle_cast/2, handle_info/2]).
-export([terminate/2, sock_opts/0, new_connection/4]).

-export([add_listener/1]).

-include("mongo.hrl").

-define(ERROR, "error\n").

-behavior(gen_nb_server).

start_link() ->
    gen_nb_server:start_link(?MODULE, []).

add_listener([IpAddr, Port]) ->
    gen_server:call(whereis(?MODULE), {add_listener, IpAddr, Port}).

remove_listener(Pid, IpAddr, Port) ->
    gen_server:call(Pid, {remove_listener, IpAddr, Port}).

init([], State) ->
    register(?MODULE, self()),
    {ok, gen_nb_server:store_cb_state([], State)}.

handle_call({add_listener, IpAddr, Port}, _From, State) ->
    [] = gen_nb_server:get_cb_state(State),
    case gen_nb_server:add_listen_socket({IpAddr, Port}, State) of
        {ok, State1} ->
            {reply, ok, State1};
        Error ->
            {reply, Error, State}
    end;
handle_call({remove_listener, IpAddr, Port}, _From, State) ->
    case gen_nb_server:remove_listen_socket({IpAddr, Port}, State) of
        {ok, State1} ->
            {reply, ok, State1};
        Error ->
            {reply, Error, State}
    end;
handle_call(_Msg, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Sock, Data}, State) ->
    Me = self(),
    P = spawn(fun() -> worker(Me, Sock, Data) end),
    gen_tcp:controlling_process(Sock, P),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

sock_opts() ->
    [binary, {active, once}, {packet, 0}].

new_connection(_IpAddr, _Port, Sock, State) ->
    Me = self(),
    P = spawn(fun() -> worker(Me, Sock) end),
    gen_tcp:controlling_process(Sock, P),
    {ok, State}.

worker(Owner, Sock) ->
    inet:setopts(Sock, [{active, once}]),
    gen_tcp:controlling_process(Sock, Owner).

worker(Owner, Sock, Data) ->
    gen_tcp:send(Sock, process_data(Sock, Data)),
    inet:setopts(Sock, [{active, once}]),
    gen_tcp:controlling_process(Sock, Owner).

process_data(Sock, ?MSG(?OP_QUERY)) ->
    process_query(Sock, ID, Rest);

process_data(_, _) ->
    ?ERROR.

process_query(Sock, ID, ?QUERY(?CMD)) ->
    process_cmd(Sock, ID, bson_binary:get_document(Rest));

process_query(_, _, _) ->
    ?ERROR.

process_cmd(Sock, ID, {{whatsmyuri, 1}, _}) ->
    {ok, {{A, B, C, D}, P}} = inet:peername(Sock), %IPv6???
    You = io_lib:format("~p.~p.~p.~p:~p", [A, B, C, D, P]),
    reply(ID, {you, list_to_binary(You), ok, 1});

process_cmd(_, _, _) ->
    ?ERROR.

reply(ID, T) ->
    Res = bson_binary:put_document(T),
    L = byte_size(Res) + 36,
    ?REPLY(L, ID, 0, ?OP_REPLY, 8, 0, 0, 1, Res).
