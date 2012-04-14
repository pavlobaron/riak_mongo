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

-module(riak_mongo_riak).

-export([insert/2]).

-include("riak_mongo_bson2.hrl").

-spec insert(bson_objectid(), bson_document()) ->
		    ok |
		    {error, too_many_fails} |
		    {error, timeout} |
		    {error, {n_val_violation, N::integer()}}.
insert(Bucket, Document) ->
    %{ok, C} = riak:local_client(),
    %O = riak_object:new(Bucket, ID, to_json(Document)),
    %C:put(O).
    to_json(Document).

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



%% internals

to_json(Document) ->


error_logger:info_msg("~p~n", [term_to_binary(Document)]),

    iolist_to_binary(mochijson2:encode(term_to_binary(Document))).

encode_document(Document) ->
    encode_element(Document).

encode_elements([]) ->
    [];
encode_elements([Element|T]) ->
    [encode_element(Element)|encode_elements(T)].

encode_element(Element) when is_atom(Element) ->


    error_logger:info_msg("atom E: ~p~n", [Element]),
    


    %S = atom_to_list(Element),
    <<"aaa">>;
encode_element(Element) when is_list(Element) ->

error_logger:info_msg("list E: ~p~n", [Element]),

    case is_tuple(lists:nth(1, Element)) of
	true -> encode_elements(Element);
	false -> Element
    end,
    encode_elements(Element);
encode_element(Element) when is_tuple(Element) ->

error_logger:info_msg("tuple E: ~p~n", [Element]),

    list_to_tuple(encode_elements(tuple_to_list(Element)));
encode_element(Element) -> Element.
