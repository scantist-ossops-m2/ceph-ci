#!/usr/bin/env bash

set -xe

client_type=$1

case ${client_type} in
	"kclient")
		echo "hello" > fscrypt_test_file
		THEN=`stat -c %z fscrypt_test_file`
		NOW=`stat -c %z fscrypt_test_file`
		echo $THEN
		echo $NOW
		while [ "$NOW" == "$THEN" ]
		do
			sleep 5
			NOW=`stat -c %z fscrypt_test_file`
			echo $NOW
		done
		if grep -q "hello" fscrypt_test_file; then
			echo "Fscrypt-protect test successfully!"
		else
			echo "Fscrypt-protect test failed!"
			exit 1
		fi
		;;
	"fuse")
		num=$(ls -l | wc -l)
		while [ $num -ne 2 ]
		do
			sleep 5
			num=$(ls -l | wc -l)
		done

		# just wait for a while to make sure the $THEN is correctly
		# updated in kernel client
		sleep 10

		set +e
		echo > ./*
		set -e
		touch *

		# wait the kclient test to finish before removing the shared
		# files when unmounting
		sleep 60

		;;
	*)
		echo "Unknown client type $1"
		exit 1
esac
