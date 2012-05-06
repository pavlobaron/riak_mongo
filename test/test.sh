#!/bin/sh

if test -z $1
then
	JS=""
	DB="test"
else
	cd mongo
	git pull origin master
	JS=$1
	DB=""
	cd jstests
fi

echo "running script? $JS"
mongo --verbose -port 32323 $DB $JS
