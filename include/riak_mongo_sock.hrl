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
%% @doc Some reusable socket stuff
%% @copyright 2012 Pavlo Baron

-define(SOCK_OPTS, [binary, {active, once}, {packet, 0}, {reuseaddr, true}]).

-define(CONTROL_MSG, {control}).
-define(CONTROLLING_PROCESS_MSG(S, O), {controlling_process, S, O}).
