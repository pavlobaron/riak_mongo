#!/bin/sh

mkdir mongo
cd mongo
git init
git config core.sparsecheckout true
echo jstests/ >> .git/info/sparse-checkout
git remote add origin https://github.com/mongodb/mongo.git
git pull origin master
