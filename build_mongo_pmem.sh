#!/bin/bash

MONGO_HOME=/home/vldb/mongo-pmem

IS_DEBUG=0

BUILD_NAME=
#BUILD_FLAGS="-Imongo/db/pmem/ -DUNIV_PMEMOBJ_BUF"
BUILD_FLAGS="-Ithird_party/wiredtiger/src/pmem/ -DUNIV_PMEMOBJ_BUF"

cd $MONGO_HOME

echo ${BUILD_FLAGS}
if [ $IS_DEBUG -eq 0 ]; then
python2 buildscripts/scons.py mongod LIBS=pmem CXXFLAGS='${BUILD_FLAGS}' -j40
else
echo "Build the Debug mode"
python2 buildscripts/scons.py mongod LIBS=pmem --gdb -j40
fi

