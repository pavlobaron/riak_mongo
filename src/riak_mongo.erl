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
%% @doc This is the main module of riak_mongo
%% @copyright 2012 Pavlo Baron

-module(riak_mongo).
-behaviour(application).
-export([start/3, start/2, start/0, stop/1]).

-define(DEFAULT_ADDR, "127.0.0.1").
-define(DEFAULT_PORT, 32323).

start(_Type, IpAddr, Port) when is_number(Port) ->
    riak_mongo_sup:start_link(IpAddr, Port);
start(_Type, IpAddr, Port) ->
    riak_mongo_sup:start_link(IpAddr, list_to_integer(Port)).

start(_Type, []) ->
    start(ignore, ?DEFAULT_ADDR, ?DEFAULT_PORT).

stop(_State) ->
    ok.

start() ->
    application:start(riak_mongo).
