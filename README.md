# Make Riak look like Mongo 

In the first step, `riak_mongo` will allow Mongo drivers to seamlessly connect to Riak using Mongo Wire Protocol and to map to the underlying Riak data store. This can help migrate the data store of existing MongoDB based applications to Riak.

## Install

To install, add the following two lines at the end of your riak's `etc/vm.args`

    -pa /path/to/riak_mongo/ebin
    -s riak_mongo

We use the low level `riak_kv` Erlang API to talk to the local store. 


## Run/Test

You can connect to it from the Mongo shell. There is a script test/test.sh which starts
the preconfigured interactive mongo shell when called without parameters:

    $ cd test
    $ ./test.sh

Then you can do some basic commands:

    > db.things.insert({a:1, b:2})
    > db.things.findOne()
    > db.things.find({a:1})
    > db.things.find({a:1}, {b:1})
    > db.things.remove({a:1})
    > db.things.remove()
	> db.things.insert({a:1})
	> x = db.things.findOne()
	> x.a = 2
	> db.things.update({_id:x._id}, x, false)

The JavaScript test suite included with the Mongo source distribution is also integrated
and runnable with riak_mongo. You need to do following:

    $ cd test
    $ ./init.sh
    $ ./test.sh update.js

Before you can use the suite, you need to initialize it. The JS code will be checked out of the Mongo github repository. You need this step only once. After that, you only need to call test.sh. Its parameter is the name of the test JS included with the Mongo distribution. Look in your test/mongo/jstests folder to find the tests you can run.

No auth, SSL and no IPv6 are considered yet.

## Mapping

Here are some details of the mapping 

- Buckets -- Objects are stored in buckets named as `"db.things"`, i.e. _database name_ . _collection name_.
- Keys -- translated so strings, `ObjectId("xx")` becomes the riak key `"OID:xx"`, similarly for UUID, and MD5 values. String keys map to themselves.  Other key types are currently not supported.
- Objects -- Stored as raw BSON using content-type `application/bson`
- Queries -- translated to map/reduce jobs that interpret the query across objects in a bucket.
- Cursors -- When a query calls for a cursor, `riak_mongo` creates a process that holds on to the query results.  These results are then simply held in the server, and fed back to the client in chunks. 

All this is work in progress, at the present state only the most basic stuff works. We're planning to also support ...
 
- Indexes -- Become 2i Riak indexes, always "_bin" indexes holding the `sext:encode` value for the corresponding BSON Erlang term.  `riak_mongo` will likely only support ascending indexes.  
- Map/reduce -- MongoDB uses runCommand to do this. We will evaluate if it makes more sense to map it to the low level Riak Erlang API or to exexute JavaScript coming from the Mongo client
- Tests -- we will provide a test suite including QuickCheck for the
  server and some automated JS tests for the client to cover both

## Authors

- Pavlo Baron (pb@pbit.org)
- Kresten Krab Thorup (krab@trifork.com)

## Contribute!

Feedback and helpers are welcome at any time.
