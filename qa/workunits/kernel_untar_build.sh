#!/usr/bin/env bash

set -ex

wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz
#wget -O linux.tar.xz https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-5.4.243.tar.xz

mkdir t
cd t
tar xzf ../linux.tar.gz
cd linux*
make defconfig
make -j`grep -c processor /proc/cpuinfo`
cd ..
if ! rm -rv linux* ; then
    echo "uh oh rm -r failed, it left behind:"
    find .
    exit 1
fi
cd ..
rm -rv t linux*
