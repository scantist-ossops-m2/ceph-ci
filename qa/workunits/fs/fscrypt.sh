#!/usr/bin/env bash

set -xe

if [ $# -ne 2 ]
then
	echo "At least 2 parameters are needed"
	exit 1
fi

# For now will test the V2 encryption policy only and the
# V1 encryption policy is deprecated

mountpoint='/tmp/fscrypt-cephfs'

# Generate a fixed keying identifier
generate_keyid()
{
	local raw=""
	local i
	for i in {1..64}; do
		raw+="\\x$(printf "%02x" $i)"
	done
	echo $raw

	# add the raw key to mountpoint it will return a keyring identifier
	keyid=$(echo -ne "$raw" | xfs_io -c "add_enckey $*" $mountpoint | awk '{print $NF}')
	echo $keyid
}

keyid=$(generate_keyid)
testdir=$2
mkdir $testdir
case $1 in
	"none")
		# do nothing for the test directory
		return
		;;
	"unlocked")
		# set encryption policy with the key provided and
		# then the test directory will be encrypted & unlocked
		echo -ne "$keyid" | xfs_io -c "set_encpolicy $*" $testdir
		return
		;;
	"locked")
		# remove the key, then the test directory will be locked
		# and any create/remove will be denied by requiring the key
		xfs_io -c "rm_enckey $keyid" $mountpoint
		return
		;;
	*)
		echo "Unknow parameter $1"
		exit 1
esac
