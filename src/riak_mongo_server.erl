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

-export([start_link/2, handle_info/2, new_connection/2, init/1, sock_opts/0]).

-export([send_packet/2]).

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
    worker_loop(Owner, Sock, <<>>).

worker_loop(Owner, Sock, UnprocessedData) ->
    receive
        {tcp, Sock, Data} ->
            {ok, Rest} = handle_data(Sock, <<UnprocessedData/binary, Data/binary>>),
            worker_loop(Owner, Sock, Rest)

        %% timeout?
    end.

handle_data(Sock, << ?get_int32(MsgLen), _/binary>>=RawData) when byte_size(RawData) >= MsgLen ->
    PacketLen = MsgLen-4,
    <<?get_int32(_), Packet:PacketLen/binary, Rest/binary>> = RawData,
    wire_protocol:process_packet(Sock, Packet),
    handle_data(Sock, Rest);

handle_data(Sock, Rest) when is_binary(Rest) ->
    inet:setopts(Sock, [{active, once}]),
    {ok, Rest}.

send_packet(Sock, Data) ->
    Size = byte_size(Data),
    gen_tcp:send(Sock, <<?put_int32(Size+4), Data>>).



