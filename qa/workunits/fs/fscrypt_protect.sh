#!/usr/bin/env bash

set -xe

client_type=$1

case ${client_type} in
	"kclient")
		mkdir fscrypt_dir
		echo "hello" > fscrypt_dir/fscrypt_test_file
		touch fscrypt_kclient_ready
		while [ ! -f fscrypt_fuse_ready ] ;
			pwd;
			ls -R;
			do sleep 1;
		done
		if grep -q "hello" fscrypt_dir/fscrypt_test_file; then
			echo "Fscrypt-protect test successfully!"
		else
			echo "Fscrypt-protect test failed!"
			exit 1
		fi
		;;
	"fuse")
		while [ ! -f fscrypt_kclient_ready ] ;
			pwd;
			ls -R;
			do sleep 1;
		done

		set +e
		echo > fscrypt_dir/*
		set -e

		touch fscrypt_fuse_ready
		;;
	*)
		echo "Unknown client type $1"
		exit 1
esac
