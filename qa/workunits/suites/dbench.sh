#!/usr/bin/env bash

set -e

testdir='./'
if [ $# -ge 1 ]
then
    testdir=$1
fi
pushd $testdir

dbench 1
dbench 10

popd
