
-record (mongo_insert, {
           request_id :: integer(),
           dbcoll :: binary(),
           continueonerror = false :: boolean(),
           documents :: [tuple()] }).

-record (mongo_update, {
           request_id :: integer(),
           dbcoll :: binary(),
           upsert = false :: boolean(),
           multiupdate = false :: boolean(),
           selector :: bson:document(),
           updater :: bson:document()  }).

-record (mongo_delete, {
           request_id :: integer(),
           dbcoll :: binary(),
           singleremove = false :: boolean(),
           selector :: bson:document() }).

-record (mongo_killcursor, {
           request_id :: integer(),
           cursorids :: [integer()] }).

-record (mongo_query, {
           request_id :: integer(),
           dbcoll :: binary(),

           tailablecursor = false :: boolean(),
           slaveok = false :: boolean(),
           nocursortimeout = false :: boolean(),
           awaitdata = false :: boolean(),
           exhaust = false :: boolean(),
           oplogreplay = false :: boolean(),
           partial = false :: boolean(),

           skip = 0 :: integer(),
           batchsize = 0 :: integer(), %% negative closes cursor
           selector :: tuple(),
           projector = [] :: bson:document() }).

-record (mongo_getmore, {
           request_id :: integer(),
           dbcoll :: binary(),
           batchsize = 0 :: integer(),
           cursorid :: integer() }).

-record (mongo_reply, {
           request_id :: integer(),
           reply_to :: integer(),
           cursornotfound = false :: boolean(),
           queryerror = false :: boolean(),
           awaitcapable = false :: boolean(),
           cursorid = 0 :: integer(),
           startingfrom = 0 :: integer(),
           documents = [] :: [bson:document()] }).

-type mongo_message() :: #mongo_insert{} | #mongo_update{} | #mongo_delete{}
                         | #mongo_killcursor{} | #mongo_query{} | #mongo_getmore{}.

