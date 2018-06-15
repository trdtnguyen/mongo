#!/bin/bash

MONGO_HOME=/home/vldb/mongo-pmem

IS_DEBUG=0

BUILD_NAME=

cd $MONGO_HOME

if [ $IS_DEBUG -eq 0 ]; then
python2 buildscripts/scons.py mongod LIBS=pmem CXXFLAGS="-DUNIV_PMEMOBJ_BUF" -j40
else
echo "Build the Debug mode"
python2 buildscripts/scons.py mongod LIBS=pmem --gdb -j40
fi

