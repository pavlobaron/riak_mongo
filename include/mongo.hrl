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

-define(MSG(OP), <<_MessageLength:32, ID:32/little, _ResponseTo:32, OP:32/little, Rest/binary>>).

-define(OP_REPLY, 1).
-define(OP_QUERY, 2004).

-define(CMD, "admin.$cmd").

-define(QUERY(C), <<_Flags:32, C, 0:8, _N1:32, _N2:32, Rest/binary>>).

-define(REPLY(L, I, T, OP, F, C, S, N, D), <<L:32/little, I:32/little, T:32/little,
					   OP:32/little, F:32/little, C:64/little,
					   S:32/little, N:32/little, D/binary>>).
