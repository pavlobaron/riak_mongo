#!/bin/sh

cd mongo
git pull origin master
cd jstests

echo "running JS test stuite"
mongo --verbose -port 32323 insert1.js insert2.js
