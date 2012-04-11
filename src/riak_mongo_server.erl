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

-include_lib ("bson/include/bson_binary.hrl").
-include ("riak_mongo_protocol.hrl").
-include_lib("riak_mongo_state.hrl").

-export([start_link/2, handle_info/2, new_connection/2, init/1, sock_opts/0]).

-behavior(gen_nb_server).

start_link(IpAddr, Port) ->
    gen_nb_server:start_link(?MODULE, IpAddr, Port, []).

init(_Args) ->
    {ok, ok}.

handle_info(_Msg, State) ->
    {noreply, State}.

new_connection(Sock, State) ->
    Me = self(),
    P = spawn(fun() -> worker(Me) end),
    gen_tcp:controlling_process(Sock, P),
    P ! {set_socket, Sock},
    {ok, State}.

sock_opts() ->
    [binary, {active, once}, {packet, 0}].

%% this should really be a process under supervision

worker(Owner) ->
    receive {set_socket, Sock} -> ok end,
    inet:setopts(Sock, [{active, once}]),
    worker_loop(#state{ owner=Owner, sock=Sock, peer=inet:peername(Sock) }, <<>>).

worker_loop(#state{sock=Sock}=State, UnprocessedData) ->
    receive
        {tcp, Sock, Data} ->
            {Messages, Rest} = riak_mongo_protocol:decode_wire(<<UnprocessedData/binary, Data/binary>>),
            State2 = process_messages(Messages, State),
            inet:setopts(Sock, [{active, once}]),
            worker_loop(State2, Rest);
        Msg ->
            error_logger:info_msg("unknown message in worker loop: ~p~n", [Msg]),
            exit(bad_msg)

        %% timeout?
    end.


%%
%% loop over messages
%%
process_messages([], State) ->
    State;
process_messages([Message|Rest], State) ->
    error_logger:info_msg("processing ~p~n", [Message]),
    case riak_mongo_message:process_message(Message, State) of
        {noreply, OutState} ->
            ok;

        {reply, Reply, #state{ sock=Sock }=State2} ->
            MessageID = element(2, Message),
            ReplyMessage = Reply#mongo_reply{ request_id = State2#state.request_id,
                                              reply_to = MessageID },
            OutState = State2#state{ request_id = (State2#state.request_id+1) },

            error_logger:info_msg("replying ~p~n", [ReplyMessage]),

            {ok, Packet} = riak_mongo_protocol:encode_packet(ReplyMessage),
            Size = byte_size(Packet),
            gen_tcp:send(Sock, <<?put_int32(Size+4), Packet/binary>>)
    end,

    process_messages(Rest, OutState);
process_messages(A1,A2) ->
    error_logger:info_msg("BAD ~p,~p~n", [A1,A2]),
    exit({badarg,A1,A2}).

