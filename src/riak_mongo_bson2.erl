%%
%% This file is part of riak_mongo
%%
%% Copyright (c) 2012 by Trifork
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

%% @author Kresten Krab Thorup
%%
%% @doc Encode/decode bson to Erlang terms.
%%
%% The reason for not using the standard erlang-bson module is that
%% (1) erlang-bson does not specify a license which leaves us in libo
%% for using it, (2) erlang-bson generates atoms for all keys in BSON
%% documents, which is highly undesirable for server-side programming
%% since atoms are not garbage collected. In this implementation keys
%% are always utf8-encoded binaries. And finally, (3) we find the
%% mochi_json-like structure easier to work with.
%%
%% @copyright 2012 Trifork

-module(riak_mongo_bson2).

-export([get_document/1, get_raw_document/1, get_cstring/1, value_type/1]).
-export([encode_document/1]).

-include("riak_mongo_bson2.hrl").

-define(DOUBLE_TAG,    16#01).
-define(STRING_TAG,    16#02).
-define(DOCUMENT_TAG,  16#03).
-define(ARRAY_TAG,     16#04).
-define(BINARY_TAG,    16#05).
-define(UNDEFINED_TAG, 16#06).
-define(OBJECTID_TAG,  16#07).
-define(BOOLEAN_TAG,   16#08).
-define(UTC_TIME_TAG,  16#09).
-define(NULL_TAG,      16#0A).
-define(REGEX_TAG,     16#0B).
-define(JAVASCRIPT_TAG,16#0D).
-define(SYMBOL_TAG,    16#0E).
-define(INT32_TAG,     16#10).
-define(INT64_TAG,     16#12).
-define(MIN_KEY_TAG,   16#FF).
-define(MAX_KEY_TAG,   16#7f).

%%
%% get and parse document from binary
%%
-spec get_document(binary()) -> {bson_document(), binary()}.
get_document(<<Size:32/little-unsigned, Rest0/binary>>) ->
    BodySize = Size-5,
    <<Body:BodySize/binary, 0, Rest/binary>> = Rest0,
    {{struct, get_elements(Body, [])}, Rest}.

%%
%% get raw document (we use this for INSERT)
%%
-spec get_raw_document(binary()) -> {bson_raw_document(), binary()}.
get_raw_document(<<Size:32/little-unsigned, _/binary>>=Binary) ->
    <<Document:Size/binary, Rest/binary>> = Binary,
    case get_raw_element(<<"_id">>, Document) of
        {ok, ID} -> ok;
        false -> ID = undefined
    end,
    {#bson_raw_document{ id=ID, body=Document}, Rest}.


get_raw_element(Bin, <<_Size:32, Body/binary>>) ->
    get_raw_element0(Bin, Body).

get_raw_element0(_,<<0>>) ->
    false;
get_raw_element0(Bin, Body) ->
    case get_element(Body) of
        {{Bin, Value}, _} ->
            {ok, Value};
        {_, Rest} ->
            get_raw_element0(Bin, Rest)
    end.


get_elements(<<>>, Acc) ->
    lists:reverse(Acc);

get_elements(Bin, Acc) ->
    {KeyValue, Rest} = get_element(Bin),
    get_elements(Rest, [KeyValue|Acc]).

-compile({inline,[get_element/1, get_element_value/2]}).

get_element(<<Tag, Rest0/binary>>) ->
    {Name, Rest1} = get_cstring(Rest0),
    {Value, Rest2} = get_element_value(Tag, Rest1),
    {{Name,Value}, Rest2}.

get_element_value(?DOUBLE_TAG, <<Value:64/float, Rest/binary>>) ->
    {Value, Rest};

get_element_value(?STRING_TAG, Rest) ->
    get_string(Rest);

get_element_value(?DOCUMENT_TAG, Rest) ->
    get_document(Rest);

get_element_value(?ARRAY_TAG, Rest0) ->
    {{struct, Elements}, Rest} = get_document(Rest0),
    {elements_to_list(Elements), Rest};

get_element_value(?BINARY_TAG, Rest) ->
    get_binary(Rest);

get_element_value(?UNDEFINED_TAG, Rest) ->
    {undefined, Rest};

get_element_value(?OBJECTID_TAG, <<ObjectID:12/binary, Rest/binary>>) ->
    {{objectid, ObjectID}, Rest};


get_element_value(?BOOLEAN_TAG, <<Bool, Rest/binary>>) ->
    case Bool of
        1 -> {true, Rest};
        0 -> {false, Rest}
    end;

get_element_value(?UTC_TIME_TAG, <<MilliSecs:64/little-unsigned, Rest/binary>>) ->
    {{MilliSecs div 1000000000, (MilliSecs div 1000) rem 1000000, (MilliSecs * 1000) rem 1000000}, Rest};

get_element_value(?NULL_TAG, Rest) ->
    {null, Rest};

get_element_value(?REGEX_TAG, Rest0) ->
    {Regex, Rest1} = get_cstring(Rest0),
    {Options, Rest} = get_cstring(Rest1),
    {{regex, Regex, Options}, Rest};

get_element_value(?JAVASCRIPT_TAG, Rest0) ->
    {Value, Rest} = get_string(Rest0),
    {{javascript, Value}, Rest};

get_element_value(?SYMBOL_TAG, Rest0) ->
    {Value, Rest} = get_string(Rest0),
    {{symbol, Value}, Rest};

get_element_value(?INT32_TAG, <<Value:32/little-signed, Rest/binary>>) ->
    {Value, Rest};

get_element_value(?INT64_TAG, <<Value:64/little-signed, Rest/binary>>) ->
    {Value, Rest};

get_element_value(?MIN_KEY_TAG, Rest) ->
    {'$min_key', Rest};

get_element_value(?MAX_KEY_TAG, Rest) ->
    {'$max_key', Rest}.


-spec get_binary(binary()) -> {bson_binary(), binary()}.
get_binary(<<Size:32/little-unsigned, SubType, Blob:Size/binary, Rest/binary>>) ->
    case SubType of
        16#00 -> Tag = binary;
        16#01 -> Tag = function;
        16#02 -> Tag = binary;
        16#03 -> Tag = uuid;
        16#04 -> Tag = md5;
        16#80 -> Tag = binary
    end,
    {{Tag, Blob}, Rest}.

-spec get_cstring(binary()) -> {bson_utf8(), binary()}.
get_cstring(Bin) ->
    {Pos, _} = binary:match(Bin, <<0>>),
    <<Value:Pos/binary, 0, Rest/binary>> = Bin,
    {Value, Rest}.

-spec get_string(binary()) -> {bson_utf8(), binary()}.
get_string(<<Length:32/little-unsigned, Bin/binary>>) ->
    StringLength = Length-1,
    <<Value:StringLength/binary, 0, Rest/binary>> = Bin,
    {Value, Rest}.

%% @doc convert `[{<<"0">>, Value1}, {<<"1">>, Value2}, ...]' to `[Value1, Value2, ...]'
-spec elements_to_list([bson_element()]) -> [bson_value()].
elements_to_list(Elements) ->
    elements_to_list(0, Elements, []).

elements_to_list(N, [{NBin, Value}|Rest], Acc) ->
    N = bin_to_int(NBin),
    elements_to_list(N+1, Rest, [Value|Acc]);
elements_to_list(_, [], Acc) ->
    lists:reverse(Acc).

bin_to_int(Bin) ->
    bin_to_int(Bin, 0).

bin_to_int(<<>>, N) ->
    N;
bin_to_int(<<CH, Rest/binary>>, N) ->
    bin_to_int(Rest, N*10 + (CH - $0)).


value_type(Value) when is_float(Value) ->
    ?DOUBLE_TAG;
value_type(Value) when is_binary(Value) ->
    ?STRING_TAG;
value_type({struct, Elems}) when is_list(Elems) ->
    ?DOCUMENT_TAG;
value_type(Value) when is_list(Value) ->
    ?ARRAY_TAG;
value_type({binary, Value}) when is_binary(Value) ->
    ?BINARY_TAG;
value_type({function, Value}) when is_binary(Value) ->
    ?BINARY_TAG;
value_type({uuid, Value}) when is_binary(Value) ->
    ?BINARY_TAG;
value_type({md5, Value}) when is_binary(Value) ->
    ?BINARY_TAG;
value_type(undefined) ->
    ?UNDEFINED_TAG;
value_type({objectid, _}) ->
    ?OBJECTID_TAG;
value_type(true) ->
    ?BOOLEAN_TAG;
value_type(false) ->
    ?BOOLEAN_TAG;
value_type({I1,I2,I3}) when is_integer(I1), is_integer(I2), is_integer(I3) ->
    ?UTC_TIME_TAG;
value_type(null) ->
    ?NULL_TAG;
value_type({regex, _,_}) ->
    ?REGEX_TAG;
value_type({javascript, _}) ->
    ?JAVASCRIPT_TAG;
value_type({symbol,_}) ->
    ?SYMBOL_TAG;
value_type(Int32) when Int32 >= -16#80000000, Int32 =< 16#7fffffff ->
    ?INT32_TAG;
value_type(Int64) when Int64 >= -16#8000000000000000, Int64 =< 16#7fffffffffffffff ->
    ?INT64_TAG;
value_type('$min_key') ->
    ?MIN_KEY_TAG;
value_type('$max_key') ->
    ?MAX_KEY_TAG.


encode_document({struct, Elements}) ->
    Body = << <<(encode_element (Key,Value)) /binary>> || {Key,Value} <- Elements>>,
    << (byte_size(Body)+5):32/unsigned-little, Body/binary, 0 >>.

encode_array(List) ->
    Elems = array_to_elements(0, List),
    encode_document({struct, Elems}).

array_to_elements(_, []) ->
    [];
array_to_elements(N, [Value|Tail]) ->
    [{ list_to_binary(integer_to_list(N)), Value } | array_to_elements(N+1, Tail)].

encode_string(String) when is_binary(String) ->
    <<(byte_size(String)+1):32/little-unsigned, String/binary, 0>>;
encode_string(String) when is_list(String) ->
    encode_string( unicode:characters_to_binary(String) );
encode_string(String) when is_atom(String) ->
    encode_string( atom_to_binary(String, utf8) );
encode_string({utf8, String}) ->
    encode_string( iolist_to_binary([String]) ).

encode_cstring(String) when is_binary(String) ->
    <<String/binary, 0>>;
encode_cstring(String) when is_list(String) ->
    encode_cstring( unicode:characters_to_binary(String) );
encode_cstring(String) when is_atom(String) ->
    encode_cstring( atom_to_binary(String, utf8) ).


encode_element(Name, Value) when is_float(Value) ->
    <<?DOUBLE_TAG, (encode_cstring(Name))/binary, Value:64/float>>;
encode_element(Name, Value) when is_binary(Value) ->
    <<?STRING_TAG, (encode_cstring(Name))/binary, (encode_string(Value))/binary >>;
encode_element(Name, {utf8,_}=Value) ->
    <<?STRING_TAG, (encode_cstring(Name))/binary, (encode_string(Value))/binary >>;
encode_element(Name, {struct, Elems}=Document) when is_list(Elems) ->
    <<?DOCUMENT_TAG, (encode_cstring(Name))/binary, (encode_document(Document))/binary >>;
encode_element(Name, Value) when is_list(Value) ->
    <<?ARRAY_TAG, (encode_cstring(Name))/binary, (encode_array(Value))/binary >>;

encode_element(Name, {binary, Value}) when is_binary(Value) ->
    <<?BINARY_TAG, (encode_cstring(Name))/binary, (byte_size(Value)):32/little, 16#00, Value>>;
encode_element(Name, {function, Value}) when is_binary(Value) ->
    <<?BINARY_TAG, (encode_cstring(Name))/binary, (byte_size(Value)):32/little, 16#01, Value>>;
encode_element(Name, {uuid, Value}) when is_binary(Value) ->
    <<?BINARY_TAG, (encode_cstring(Name))/binary, (byte_size(Value)):32/little, 16#03, Value>>;
encode_element(Name, {md5, Value}) when is_binary(Value) ->
    <<?BINARY_TAG, (encode_cstring(Name))/binary, (byte_size(Value)):32/little, 16#04, Value>>;

encode_element(Name, undefined) ->
    <<?UNDEFINED_TAG, (encode_cstring(Name))/binary>>;

encode_element(Name, {objectid, ID}) ->
    <<?OBJECTID_TAG, (encode_cstring(Name))/binary, ID/binary>>;

encode_element(Name, true) ->
    <<?BOOLEAN_TAG, (encode_cstring(Name))/binary, 1>>;

encode_element(Name, false) ->
    <<?BOOLEAN_TAG, (encode_cstring(Name))/binary, 0>>;

encode_element(Name, {MegaSecs, Secs, MicroSecs})
  when is_integer(MegaSecs), is_integer(Secs), is_integer(MicroSecs) ->
    <<?UTC_TIME_TAG, (encode_cstring(Name))/binary, (MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000):64/little>>;

encode_element(Name, null) ->
    <<?NULL_TAG, (encode_cstring(Name))/binary>>;

encode_element(Name, {regex, Regex, Options}) ->
    <<?REGEX_TAG, (encode_cstring(Name))/binary, Regex/binary, 0, Options/binary, 0>>;

encode_element(Name, {javascript, String}) ->
    <<?JAVASCRIPT_TAG, (encode_cstring(Name))/binary, (encode_string(String))/binary >>;

encode_element(Name, {symbol, String}) ->
    <<?SYMBOL_TAG, (encode_cstring(Name))/binary, (encode_string(String))/binary >>;

encode_element(Name, Int32) when Int32 >= -16#80000000, Int32 =< 16#7fffffff ->
    <<?INT32_TAG, (encode_cstring(Name))/binary, Int32:32/little-signed>>;
encode_element(Name, Int64) when Int64 >= -16#8000000000000000, Int64 =< 16#7fffffffffffffff ->
    <<?INT64_TAG, (encode_cstring(Name))/binary, Int64:64/little-signed>>;
encode_element(Name, '$min_key') ->
    <<?MIN_KEY_TAG, (encode_cstring(Name))/binary>>;
encode_element(Name, '$max_key') ->
    <<?MAX_KEY_TAG, (encode_cstring(Name))/binary>>.
