#!/bin/sh

if test -z $1
then
	JS=""
else
	cd mongo
	git update
	JS=$1
	cd jstests
fi

echo "running script? $JS"
mongo --verbose -port 32323 collection $JS
