#!/usr/bin/env bash
#
# Copyright (C) 2015 Intel <contact@intel.com.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
#
# Author: Xiaoxi Chen <xiaoxi.chen@intel.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7144" # git grep '\<7144\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_recover_unexpected() {
    local dir=$1
    local poolname=pl1
    local objname=obj1
    local snapname=sn1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    ceph osd pool create $poolname 1
    rados -p $poolname put $objname /etc/passwd
    rados -p $poolname mksnap $snapname
    rados -p $poolname put $objname /etc/group

    wait_for_clean || return 1

    local osd=$(get_primary $poolname $objname)
    JSON=`objectstore_tool $dir $osd --op list $objname | grep snapid.:1`
    echo "JSON is $JSON"
    rm -f $dir/_ $dir/data
    objectstore_tool $dir $osd "$JSON" get-attr _ > $dir/_ || return 1
    objectstore_tool $dir $osd "$JSON" get-bytes $dir/data || return 1

    rados -p $poolname rmsnap $snapname

    sleep 5
    objectstore_tool $dir $osd --op list $objname > /tmp/obj_before_repair.txt
    objectstore_tool $dir $osd "$JSON" set-bytes $dir/data || return 1
    objectstore_tool $dir $osd "$JSON" set-attr _ $dir/_ || return 1

    sleep 5

    ceph pg repair 1.0 || return 1

    sleep 10

    ceph log last
    cp $dir/osd.?.log /tmp
    objectstore_tool $dir $osd --op list $objname > /tmp/obj_after_repair.txt

    # make sure osds are still up
    timeout 60 ceph tell osd.0 version || return 1
    timeout 60 ceph tell osd.1 version || return 1
    timeout 60 ceph tell osd.2 version || return 1
}


main osd-unexpected-clone "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bench.sh"
# End:
