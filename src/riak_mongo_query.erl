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
%% @doc Match mongo queries
%%
%% @copyright 2012 Trifork

-module(riak_mongo_query).

-include("riak_mongo_bson.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([matches/2, compile/1]).

-define(CQ, '$compiled_mongo_query').

-type query_key()   :: bson_utf8() | atom().
-type query_value() :: {re_pattern,_,_,_} | bson_value() | compiled_mongo_query().
-type compiled_mongo_query() :: {?CQ, [{query_key(),query_value()}]}.

-spec matches(bson_document(), bson_document()|compiled_mongo_query()) -> true|false.
matches({struct, DocElems}, Query) ->
    {?CQ, QueryElems} = compile(Query),
    lists:all(fun({QueryKey, QueryValue}) ->
                      doc_match(DocElems, QueryKey, QueryValue)
              end,
              QueryElems).


doc_match(DocElems, '$or', Alternatives) ->
    lists:any(fun({QueryKey, QueryValue}) ->
                      doc_match(DocElems, QueryKey, QueryValue)
              end,
              Alternatives);

doc_match(DocElems, '$and', Alternatives) ->
    lists:all(fun({QueryKey, QueryValue}) ->
                      doc_match(DocElems, QueryKey, QueryValue)
              end,
              Alternatives);

doc_match(DocElems, QueryKey, QueryValue) ->
    error_logger:info_msg("matching ~p vs {~p,~p}~n", [DocElems,QueryKey,QueryValue]),
    case lists:keyfind(QueryKey, 1, DocElems) of
        {_, _}=DocElem ->
            elem_match(DocElem, QueryKey, QueryValue);
        false ->
            case QueryValue of
                {?CQ, [{'$exists', false}]} ->
                    true;
                _ ->
                    false
            end
    end.



elem_match({_Key,Value}, '$gt',  QueryValue) ->
    compare(Value, '>', QueryValue);

elem_match({_Key,Value}, '$lt',  QueryValue) ->
    compare(Value, '<', QueryValue);

elem_match({_Key,Value}, '$gte', QueryValue) ->
    compare(Value, '>=', QueryValue);

elem_match({_Key,Value}, '$lte', QueryValue) ->
    compare(Value, '=<', QueryValue);

elem_match({_Key,Value}, '$eq', QueryValue) ->
    compare(Value, '=:=', QueryValue);

elem_match({_Key,Value}, '$ne', QueryValue) ->
    compare(Value, '=/=', QueryValue);

elem_match({_Key,Value}, '$size', QueryValue) when length(Value) == QueryValue ->
    true;

elem_match({_Key,Value}, '$type', QueryValue) ->
    riak_mongo_bson:value_type(Value) =:= QueryValue;

elem_match(_KeyValue, '$exists', Bool) when is_boolean(Bool) ->
    Bool;

elem_match({_Key,Value}, '$in',  QueryValue) when is_list(QueryValue) ->
    lists:member(Value, QueryValue);

elem_match({_Key,Value}, '$nin',  QueryValue) when is_list(QueryValue) ->
    not lists:member(Value, QueryValue);

elem_match(KeyValue, QueryOp, QueryValue) when is_atom(QueryOp) ->
    error_logger:error_msg("unhandled operator ~p ~p ~p~n", [KeyValue,QueryOp,QueryValue]),
    false;


elem_match({Key,Value}, Key, {re_pattern,_,_,_}=MP) ->
    case re:run([Value], MP) of
        nomatch -> false;
        {match,_} -> true
    end;

elem_match({Key,Value}, Key, Value) ->
    true;

elem_match({Key,_}=KeyValue, Key, {?CQ, QueryElems}) ->
    lists:all(fun({QueryKey, QueryValue}) ->
                      elem_match(KeyValue, QueryKey, QueryValue)
              end,
              QueryElems);


%% here, we probably need to handle som conversions...

elem_match(_,_,_) ->
    false.


%%
%% TODO: figure out if we need to coerce values, e.g.  " 1 < '2' "
%% TODO: figure out how to compare unicode strings
%%
compare(Value1,Op,Value2) ->
    erlang:Op(Value1,Value2).


-spec compile(bson_document()|compiled_mongo_query()) -> compiled_mongo_query().

compile({?CQ, _}=CompiledQuery) ->
    CompiledQuery;
compile({struct, Elems}) ->
    {?CQ, lists:map(fun({<<$$,_/binary>>=Key,Value}) ->
                            {binary_to_existing_atom(Key,utf8), compile(Value)};
                       ({Key,Value}) ->
                            case binary:split(Key, <<".">>) of
                                [_] ->
                                    {Key, compile(Value)};
                                [Head,Rest] ->
                                    {Head, compile({struct, [{Rest,Value}]})}
                            end
                    end,
                    Elems)};

compile({regex, Regex, Options}) ->
    More = lists:map(fun($i) -> caseless;
                        ($m) -> multiline;
                        ($g) -> global;
                        (Opt)  -> error(bad_option_to_regex, <<Opt>>)
                     end,
                     binary_to_list(Options)),
    {ok, RE} = re:compile(Regex, [unicode|More]),
    RE;

compile(List) when is_list(List) ->
    lists:map(fun compile/1, List);

compile(Value) ->
    Value.




-ifdef(TEST).

match_test() ->
    Document = {struct, [{<<"a">>, 4}, {<<"s">>, <<"peter">>}]},
    ?assert( matches(Document, Document) ),
    ?assert( matches(Document, {struct,
                                [{<<"a">>, {struct,
                                            [{<<"$gt">>, 2}]}}]
                               }) ),
    ?assert(not matches(Document, {struct,
                                   [{<<"a">>, {struct,
                                               [{<<"$gt">>, 6}]}}]
                                  }) ),
    ?assert(matches(Document, {struct,
                               [{<<"b">>, {struct,
                                           [{<<"$exists">>, false}]}}]})),
    ?assert(not matches(Document, {struct,
                                   [{<<"a">>, {struct,
                                               [{<<"$exists">>, false}]}}]})),

    ?assert(matches(Document, {struct,
                               [{<<"s">>, {regex, <<"e">>, <<"">>}}]})),

    ?assert(not matches(Document, {struct,
                               [{<<"s">>, {regex, <<"x">>, <<"">>}}]})),

    ?assert(matches(Document, {struct,
                               [{<<"s">>, {regex, <<"E">>, <<"i">>}}]})),


    ok.

-endif.



