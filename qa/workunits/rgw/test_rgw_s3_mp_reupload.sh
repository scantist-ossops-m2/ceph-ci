#!/usr/bin/env bash

# if there's a command-line argument, treat it as the RGW port #

# exit script if subcommand fails
set -e

mydir=$(dirname $0)
data_pool=default.rgw.buckets.data
orphan_list_out=/tmp/olo.$$

# install boto3
python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install boto3

rgw_host="$(hostname --fqdn)"
echo "INFO: fully qualified domain name: $rgw_host"

export RGW_ACCESS_KEY="0555b35654ad1656d804"
export RGW_SECRET_KEY="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
export RGW_HOST="$rgw_host"
export RGW_PORT="${1:-80}"

# create user if they don't already exit
if radosgw-admin user info --access-key $RGW_ACCESS_KEY ;then
    echo INFO: user already exists
else
    echo INFO: creating user
    radosgw-admin user create --uid testid \
		  --access-key $RGW_ACCESS_KEY --secret $RGW_SECRET_KEY \
		  --display-name 'M. Tester' --email tester@ceph.com
fi

# create a randomized bucket name
bucket="bkt$(expr $(date +%s) % 999800 + 111)"

$mydir/bin/python3 ${mydir}/test_rgw_s3_mp_reupload.py $bucket

marker=$(radosgw-admin metadata get bucket:$bucket | grep bucket_id | sed 's/.*: "\(.*\)".*/\1/')

echo "radosgw-admin bucket radoslist:"
radosgw-admin bucket radoslist --bucket=$bucket | sort

echo "rados ls:"
rados ls -p $data_pool | sort

used=$(radosgw-admin bucket radoslist --bucket=$bucket | wc -l)
present=$(rados ls -p $data_pool | grep "^$marker" | wc -l)

if [ "$used" -ne "$present" ] ;then
   echo "ERROR: expecting $used rados objects in data pool but found $present"
   error=1
fi

# for now let's not do rgw-orphan-list since the output is very noisy
if false ;then
    rgw-orphan-list $data_pool > $orphan_list_out
    if grep -q "^0 potential orphans found" $orphan_list_out ;then
	:
    else
	echo "ERROR: $(grep 'potential orphans found' $orphan_list_out)"
	error=1
    fi
    rm -f $orphan_list_out
fi

# CLEAN UP
deactivate

if [ -z "$error" ] ;then
    echo OK.
else
    exit 1
fi
