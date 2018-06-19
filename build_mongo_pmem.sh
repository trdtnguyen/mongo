#!/bin/bash

MONGO_HOME=/home/vldb/mongo-pmem

IS_DEBUG=0

BUILD_NAME=
#BUILD_FLAGS="-Imongo/db/pmem/ -DUNIV_PMEMOBJ_BUF"
#BUILD_FLAGS="-Ithird_party/wiredtiger/src/pmem/"
BUILD_FLAGS="-Ithird_party/wiredtiger/src/pmem/ -DUNIV_PMEMOBJ_BUF -DUNIV_PMEMOBJ_BUF_FLUSHER -DUNIV_PMEMOBJ_BUF_PARTITION"

cd $MONGO_HOME

echo ${BUILD_FLAGS}
#python2 buildscripts/scons.py mongod --gdb -j40 LIBS=pmem CPPDEFINES=UNIV_PMEMOBJ_BUF CPPPATH=third_party/wiredtiger/src/pmem/ CXXFLAGS='${BUILD_FLAGS}' --prefix=${MONGO_HOME}
#python2 buildscripts/scons.py mongod --gdb -j40 LIBS='pmem pmemobj' CPPPATH=/home/vldb/mongo-pmem/src/third_party/wiredtiger/src/pmem/ --prefix=${MONGO_HOME}
python2 buildscripts/scons.py mongod --gdb -j40 LIBS='pmem pmemobj' --prefix=${MONGO_HOME}
#python2 buildscripts/scons.py mongod --gdb -j40 --prefix=${MONGO_HOME}
#python2 buildscripts/scons.py --gdb mongod install LIBS=pmem CXXFLAGS='${BUILD_FLAGS}' -prefix=$MONGO_HOME -j40
#python2 buildscripts/scons.py -j40 install

cp build/opt/mongo/mongod .
cp build/opt/mongo/mongo .
cp build/opt/mongo/mongos .

#if [ $IS_DEBUG -eq 0 ]; then
#python2 buildscripts/scons.py mongod LIBS=pmem CXXFLAGS='${BUILD_FLAGS}' -j40
#else
#echo "Build the Debug mode"
#python2 buildscripts/scons.py mongod LIBS=pmem --gdb -j40
#fi

