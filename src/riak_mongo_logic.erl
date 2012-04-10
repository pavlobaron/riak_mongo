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
%% @doc This is the central logic module of riak_mongo
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_logic).

-export([you/1, find/3, insert/2]).

you(Peer) ->
    {ok, {{A, B, C, D}, P}} = Peer, %IPv6???
    io_lib:format("~p.~p.~p.~p:~p", [A, B, C, D, P]).

insert(Collection, Document) ->
    {ok, C} = riak:local_client(),
    case bson:lookup ('_id', Document) of
        {ID} ->
            Doc = Document;
        {} ->
            ID = list_to_binary(riak_core_util:unique_id_62()),
            Doc = bson:append({'_id', ID}, Document)
    end,
    O = riak_object:new(Collection, ID, Doc),
    C:put(O).

find(Collection, NumberToReturn, _) when NumberToReturn == -1 ->
    {ok, C} = riak:local_client(),
    {ok, L} = C:list_keys(Collection),
    case L of
	[] -> {};
	[Key|_] ->
	    {ok, O} = C:get(Collection, Key),
	    riak_object:get_value(O);
	_ -> unsupported
    end;

find(_, _, _) ->
    unsupported.
