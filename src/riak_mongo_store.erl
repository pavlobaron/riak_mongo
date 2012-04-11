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
%% @doc Here we speak to the Riak store
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_store).

-export([insert/2]).

-spec insert(binary(), tuple()) -> term().
insert(Bucket, {ID, Struct}) ->
    {ok, C} = riak:local_client(),
    O = riak_object:new(Bucket, ID, riak_mongo_decenc:encode_struct(Struct)),
    C:put(O).

%find(#mongo_query=Query) ->
    % query!!!
    % if NumberToReturn < 0 then we close the cursor (cursor? state?)
%    {ok, C} = riak:local_client(),
%    {ok, L} = C:list_keys(Collection),
%    collect_objects(C, L).

%collect_objects(_, []) ->
%    [];
%collect_objects(C, [Key|L]) ->
%    {ok, O} = C:get(Collection, Key),
%    [riak_object:get_value(O)|collect_objects(C, L)].
