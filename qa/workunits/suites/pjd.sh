#!/usr/bin/env bash

set -e

testdir='./'
if [ $# -ge 1 ]
then
    testdir=$1
fi
pushd $testdir

wget http://download.ceph.com/qa/pjd-fstest-20090130-RC-aclfixes.tgz
tar zxvf pjd*.tgz
cd pjd-fstest-20090130-RC
make clean
make
cd ..
mkdir tmp
cd tmp
# must be root!
sudo prove -r -v --exec 'bash -x' ../pjd*/tests
cd ..
rm -rf tmp pjd*

popd
