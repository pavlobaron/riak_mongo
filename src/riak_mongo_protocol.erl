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

%% @author Kresten Krab Thorup <krab@trifork.com>
%% @doc This module decomposes mongo wire messages
%% @copyright 2012 Trifork
-module(riak_mongo_protocol).

-export([decode_wire/1, decode_packet/1]).
-export([split_dbcoll/1]).

-include_lib ("bson/include/bson_binary.hrl").
-include_lib ("riak_mongo_protocol.hrl").

-define (ReplyOpcode, 1).
-define (UpdateOpcode, 2001).
-define (InsertOpcode, 2002).
-define (QueryOpcode, 2004).
-define (GetmoreOpcode, 2005).
-define (DeleteOpcode, 2006).
-define (KillcursorOpcode, 2007).


%% @doc
%% Decode bytes straight off the wire, including packet length headers.

-spec decode_wire( binary() ) -> { [ mongo_message() ], binary() }.

decode_wire(Binary) ->
    decode_wire(Binary, []).

decode_wire(<<?get_int32(Size), Rest/binary>>, Acc) when (byte_size(Rest)+4) >= Size ->
    <<Packet:Size/binary, NextRest/binary>> = Rest,
    {ok, Message} = decode_packet(Packet),
    decode_wire( NextRest, [Message|Acc] );

decode_wire(Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

-define(HDR(ResponseTo, Opcode), RequestId:32/little, ResponseTo:32/little, Opcode:32/little).

-spec decode_packet( binary() ) -> {ok, mongo_message() }.

bool(1) -> true;
bool(0) -> false.

decode_packet( << ?HDR(_, ?InsertOpcode), ?get_bits32(0,0,0,0,0,0,0,ContinueOnError), Rest/binary >> ) ->
    {DBColl, Rest1} = bson_binary:get_cstring(Rest),
    BsonDocs = get_all_docs(Rest1),
    {ok, #mongo_insert{ dbcoll=DBColl,
                        request_id=RequestId,
                        documents=BsonDocs,
                        continueonerror = bool(ContinueOnError)
                      }};

decode_packet(<< ?HDR(_, ?UpdateOpcode), 0:32, Rest/binary>> ) ->
    {DBColl, Rest1} = bson_binary:get_cstring(Rest),
    <<?get_bits32(0,0,0,0,0,0,MultiUpdate,Upsert), Rest2>> = Rest1,
    {Selector, Rest3} = bson_binary:get_document(Rest2),
    {Update, <<>>} = bson_binary:get_document(Rest3),

    {ok, #mongo_update{ dbcoll=DBColl,
                        request_id=RequestId,
                        selector=Selector,
                        updater=Update,

                        multiupdate = bool(MultiUpdate),
                        upsert = bool(Upsert)
                      }};

decode_packet(<< ?HDR(_, ?DeleteOpcode), 0:32, Rest/binary >>) ->
    {DBColl, Rest1} = bson_binary:get_cstring(Rest),
    <<?get_bits32(0,0,0,0,0,0,0,SingleRemove), Rest2>> = Rest1,
    {Selector, <<>>} = bson_binary:get_document(Rest2),

    {ok, #mongo_delete{ dbcoll=DBColl,
                        request_id=RequestId,
                        selector=Selector,
                        singleremove=bool(SingleRemove) }};

decode_packet(<< ?HDR(_, ?KillcursorOpcode), 0:32, ?get_int32(NumCursorIDs), Rest/binary >> ) ->
    { IDs, <<>> } = get_int64_list(NumCursorIDs, Rest),
    {ok, #mongo_killcursor{ request_id=RequestId,
                            cursorids=IDs } };

decode_packet(<< ?HDR(_, ?QueryOpcode),
                 ?get_bits32(Partial,Exhaust,AwaitData,NoCursorTimeout,OplogReplay,SlaveOK,Tailable,0),
                 Rest/binary >>) ->
    {DBColl, Rest1} = bson_binary:get_cstring(Rest),
    << ?get_int32(NumberToSkip), ?get_int32(NumberToReturn), Rest2 >> = Rest1,
    [Query | ReturnFieldSelectors ] = get_all_docs(Rest2),

    {ok, #mongo_query{ request_id=RequestId,
                       dbcoll=DBColl,
                       tailablecursor=bool(Tailable),
                       slaveok=bool(SlaveOK),
                       nocursortimeout=bool(NoCursorTimeout),
                       awaitdata=bool(AwaitData),
                       exhaust=bool(Exhaust),
                       partial=bool(Partial),
                       oplogreplay=bool(OplogReplay),
                       skip=NumberToSkip,
                       batchsize=NumberToReturn,
                       selector=Query,
                       projector=ReturnFieldSelectors }};

decode_packet(<< ?HDR(_, ?GetmoreOpcode), 0:32, Rest/binary >>) ->
    {DBColl, Rest1} = bson_binary:get_cstring(Rest),
    << ?get_int32(NumberToReturn), ?get_int64(CursorID) >> = Rest1,

    {ok, #mongo_getmore{ request_id=RequestId,
                         dbcoll=DBColl,
                         batchsize=NumberToReturn,
                         cursorid=CursorID }};

decode_packet(<< ?HDR(_,OP), _/binary >>) ->
    exit({error, {bad_message, RequestId, OP}}).


split_dbcoll(Bin) ->
	{Pos, _Len} = binary:match (Bin, <<$.>>),
	<<DB :Pos /binary, $.:8, Coll /binary>> = Bin,
	{DB, Coll}.


%%
%%
%%
get_all_docs(Binary) ->
    get_all_docs(Binary, []).

get_all_docs(<<>>, Acc) ->
    lists:reverse(Acc);
get_all_docs(Data, Acc) ->
    {Doc, Rest} = bson_binary:get_document(Data),
    get_all_docs(Rest, [Doc|Acc]).


get_int64_list(Num, Binary) ->
    get_int64_list(Num, Binary, []).

get_int64_list(0, Rest, Acc) ->
    { lists:reverse(Acc), Rest };
get_int64_list(N, << ?get_int64(Value), Rest/binary >>, Acc) ->
    get_int64_list(N-1, Rest, [Value | Acc ]).
