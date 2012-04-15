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
%% @doc This is the TCP server of riak_mongo
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_server).

-export([start_link/2, init/1, handle_info/2]).
-export([new_connection/2, sock_opts/0]).

-behavior(gen_nb_server).

-include("riak_mongo_state.hrl").
-include("riak_mongo_sock.hrl").

start_link(IpAddr, Port) ->
    gen_nb_server:start_link(?MODULE, IpAddr, Port, []).

init(_Args) ->
    {ok, ok}.

new_connection(Sock, State) ->
    error_logger:info_msg("New connection: ~p, ~p~n", [self(), Sock]),

    riak_mongo_worker_sup:new_worker(Sock, self()),
    {ok, State}.

sock_opts() -> ?SOCK_OPTS.

handle_info(?CONTROLLING_PROCESS_MSG(Sock, NewOwner), State) ->
    error_logger:info_msg("Handing over control from to ~p on sock ~p~n", [NewOwner, Sock]),

    gen_tcp:controlling_process(Sock, NewOwner),
    NewOwner ! ?CONTROL_MSG,
    {noreply, State};

handle_info(Msg, State) ->
    error_logger:info_msg("unknown message in worker callback: ~p~n", [Msg]),
    {noreply, State}.
