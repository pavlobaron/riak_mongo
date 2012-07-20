#!/bin/sh

if test -z $2
then
	GIT=""
else
	GIT="git pull origin master"
fi

if test -z $1
then
	JS=""
	DB="test"
else
	cd mongo
	`$GIT`
	JS=$1
	DB=""
	cd jstests
fi

echo "running script? $JS"
mongo --verbose -port 32323 $DB $JS
