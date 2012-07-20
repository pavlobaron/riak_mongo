#!/bin/sh

if test -z $1
then
        GIT=""
else
        GIT="git pull origin master"
fi

cd mongo
`$GIT`
cd jstests

echo "running JS test stuite"
mongo --verbose -port 32323 insert1.js insert2.js
