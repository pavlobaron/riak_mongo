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

%% @author Pavlo Baron <pbat pbit dot org> (borrowed from Kevin Smith)
%% @doc This is the mail supervisor of riak_mongo
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([add_listener/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    ServerSpec = {server,
                  {riak_mongo_server, start_link, []},
                  transient, 2000, worker, [riak_mongo_server]},
    {ok, {{one_for_one, 2, 10}, [ServerSpec]}}.

add_listener(Args) ->
    riak_mongo_server:add_listener(Args).
