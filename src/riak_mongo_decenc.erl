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
%% @doc Utils around mongo-bson and JSON encoding/decoding
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_decenc).

-export([encode_struct/1, decode_bson/1]).

-include_lib("bson/include/bson_binary.hrl").

-spec decode_bson(binary()) -> {binary(), binary()}.
decode_bson(<<>>) ->
    [];
decode_bson(<<?get_int32(N), RawBson/binary>>) ->
    S = N - 5,
    <<DB:S/binary, 0:8, Rest/binary>> = RawBson,
    RawStruct = do_fields(DB),
    TID = lists:keyfind(<<"_id">>, 1, RawStruct),
    ID = case is_tuple(TID) of
	     true -> element(2, TID);
	     _ -> <<>>
	 end,
    [{ID, {struct, RawStruct}}|decode_bson(Rest)].

do_fields(<<>>) -> [];
do_fields(B) ->
    {N, V, B1} = bson_binary:get_field(B),
    [{N, V} | do_fields(B1)].

-spec encode_struct(tuple()) -> binary().
encode_struct(Struct) ->


error_logger:info_msg("~p", [mochijson2:encode(Struct)]),

    iolist_to_binary(mochijson2:encode(Struct)).
