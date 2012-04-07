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
%% @doc This is the implementiation of the Mongo Wire protocol
%% @copyright 2012 Pavlo Baron

-module(wire_protocol).

-export([process_data/2]).

-define(MSG(OP), <<_MessageLength:32, ID:32/little, _ResponseTo:32, OP:32/little, Rest/binary>>).

-define(OP_REPLY, 1).
-define(OP_QUERY, 2004).

-define(CMD, "admin.$cmd").

-define(QUERY(C), <<_Flags:32, C, 0:8, _N1:32, _N2:32, Rest/binary>>).

-define(REPLY(L, I, T, OP, F, C, S, N, D), <<L:32/little, I:32/little, T:32/little,
					   OP:32/little, F:32/little, C:64/little,
					   S:32/little, N:32/little, D/binary>>).

%{{errmsg,<<"not running with --replSet">>,ok,0.0},<<>>}

process_data(Sock, ?MSG(?OP_QUERY)) ->
    process_query(Sock, ID, Rest);

process_data(_, _) ->
    reply_error(0, "unsupported message").

process_query(Sock, ID, ?QUERY(?CMD)) ->
    process_cmd(Sock, ID, bson_binary:get_document(Rest));

process_query(_, _, _) ->
    reply_error(0, "unsupported query").

process_cmd(Sock, ID, {{whatsmyuri, 1}, _}) ->
    {ok, {{A, B, C, D}, P}} = inet:peername(Sock), %IPv6???
    You = io_lib:format("~p.~p.~p.~p:~p", [A, B, C, D, P]),
    reply(ID, {you, list_to_binary(You), ok, 1});

process_cmd(_, ID, {{replSetGetStatus, 1, forShell, 1}, _}) ->
    reply_error(ID, "not running with --replSet");

process_cmd(_, _, _) ->
    reply_error(0, "unsupported command").

reply(ID, T) ->
    Res = bson_binary:put_document(T),
    L = byte_size(Res) + 36,
    ?REPLY(L, ID, ID, ?OP_REPLY, 8, 0, 0, 1, Res).

reply_error(ID, S) ->
    T = {errmsg, list_to_binary(S), ok, 0},
    reply(ID, T).
