%%
%% This file is part of riak_mongo
%%
%% Copyright (c) 2012 by Ward Bekker (ward at equanimity dot nl)
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

%% @author Ward Bekker
%%
%% @doc Javascript value interpretation emulation
%%
%% @copyright 2012 by Ward Bekker (ward at equanimity dot nl)

-module(riak_mongo_js_emulation).

-export([is_true/1]).

is_true(undefined) -> false;
is_true(null) -> false;
is_true(false) -> false;
is_true(0) -> false;
is_true(0.0) -> false;
is_true(_) -> true.
