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
%% @doc This is the wrapper around the mongo-bson encoder/decoder
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_bson).

-export([encode_bson/1, decode_bson/1]).

-include_lib("bson/include/bson_binary.hrl").

-spec decode_bson(binary()) -> {binary(), binary()}.
decode_bson(<<?get_int32(N), RawBson/binary>>) ->
    S = N - 5,
    <<DB:S/binary, 0:8, _Rest/binary>> = RawBson,
    RawStruct = do_fields(DB),
    ID = [V || {K, V} <- RawStruct, K =:= <<"_id">>],
    {list_to_binary(ID), {struct, RawStruct}}.

do_fields(<<>>) -> [];
do_fields(B) ->
    {N, V, B1} = bson_binary:get_field(B),
    [{N, V} | do_fields(B1)].

-spec encode_bson(binary()) -> binary().
encode_bson(Struct) ->
    iolist_to_binary(mochijson2:encode(Struct)).
