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
%% @doc This is the worker
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_worker).

-export([start_link/2, handle_info/2, init/1]).

-behavior(gen_server).

-include("riak_mongo_protocol.hrl").
-include("riak_mongo_state.hrl").
-include("riak_mongo_sock.hrl").

start_link(Sock, Owner) ->
    gen_server:start_link(?MODULE, [Sock, Owner], []).

init([Sock, Owner]) ->
    error_logger:info_msg("Requested to hand over control on sock ~p, ~p~n", [Sock, Owner]),

    Owner ! ?CONTROLLING_PROCESS_MSG(Sock, self()),
    InitBin = <<>>,
    {ok, #worker_state{sock=Sock, rest=InitBin}}.

handle_info({tcp, Sock, Data}, State) ->
    error_logger:info_msg("Starting to proceess message: ~p, ~p, ~p~n", [Sock, Data, State#worker_state.rest]),

    UnprocessedData = State#worker_state.rest,
    {Messages, Rest} = riak_mongo_protocol:decode_wire(<<UnprocessedData/binary, Data/binary>>),
    State2 = riak_mongo_message:process_messages(Messages, State),
    inet:setopts(Sock, ?SOCK_OPTS),
    State3 = State2#worker_state{rest=Rest},
    {noreply, State3};

handle_info({tcp_closed, _Sock}, State) -> {noreply, State};

handle_info(?CONTROL_MSG, State) ->
    error_logger:info_msg("Having control: ~p~n", [State#worker_state.sock]),

    inet:setopts(State#worker_state.sock, ?SOCK_OPTS),
    {noreply, State};


handle_info({'DOWN',Ref,_,_,_}, State=#worker_state{ cursors=CursorDict }) ->

    Dict2 = dict:filter(fun(_, {MRef,_}) ->
                                MRef =/= Ref
                        end,
                        CursorDict),

    {noreply, State#worker_state{ cursors=Dict2 }};


handle_info(Msg, State) ->
    error_logger:info_msg("unknown message in worker callback: ~p~n", [Msg]),
    {noreply, State}.
