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

-include("riak_mongo_bson2.hrl").
-include("riak_mongo_protocol.hrl").
-include("riak_mongo_state.hrl").

-export([insert/2]).

insert(#mongo_insert{dbcoll=Collection, documents=Docs, continueonerror=ContinueOnError}, State) ->

    {ok, C} = riak:local_client(),

    Errors =
        lists:foldl(fun(#bson_raw_document{ id=BSON_ID, body=Doc }, Err)
                          when Err=:=[]; ContinueOnError=:=true ->
                            ID = bson_to_riak_key(BSON_ID),

                            O = riak_object:new(Collection, ID, Doc, "application/bson"),

                            error_logger:info_msg("storing ~p~n", [O]),

                            case C:put(O) of
                                ok -> Err;
                                Error -> [Error|Err]
                            end
                    end,
                    [],
                    Docs),

    State#state{ lastError=Errors }.

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


bson_to_riak_key({objectid, BIN}) ->
    iolist_to_binary("OID:" ++ hexencode(BIN)).

hexencode(<<>>) -> [];
hexencode(<<CH, Rest/binary>>) ->
    [ hex(CH) | hexencode(Rest) ].

hex(CH) when CH < 16 ->
    [ $0, integer_to_list(CH, 16) ];
hex(CH) ->
    integer_to_list(CH, 16).


