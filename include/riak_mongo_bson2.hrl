

-record(bson_raw_document, { id :: bson_value(), body :: binary() }).
-type bson_raw_document() :: #bson_raw_document{}.

-type bson_document()   :: {struct, [bson_element()]}.
-type bson_element()    :: {bson_utf8(), bson_value()}.
-type bson_utf8()       :: binary().
-type bson_objectid()   :: {objectid, binary()}.
-type bson_regex()      :: {regex, bson_utf8(), bson_utf8()}.
-type bson_javascript() :: {javascript, bson_utf8()}.
-type bson_symbol()     :: {symbol, bson_utf8()}.
-type bson_binary()     :: {binary|function|uuid|md5, binary()}.
-type bson_integer()    :: -16#8000000000000000 .. 16#7fffffffffffffff.
-type bson_time()       :: { integer(), integer(), integer() }.
-type bson_value()      ::
        boolean()
      | float()
      | undefined | null
      | bson_integer()
      | bson_document()
      | bson_utf8()
      | [bson_value()]
      | bson_objectid()
      | bson_regex()
      | bson_javascript()
      | bson_symbol()
      | bson_binary()
      | bson_time()
      | '$min_key' | '$max_key'
      .

