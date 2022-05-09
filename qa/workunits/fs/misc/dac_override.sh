#!/bin/sh -x

set -e

mkdir -p testdir

file=test_chmod.$$

echo "foo" > testdir/${file}
if test $? != 0; then
	echo "ERROR: Failed to create file ${file}"
	exit 1
fi

sudo chmod 600 testdir
if test $? != 0; then
	echo "ERROR: Failed to change mode of ${file}"
	exit 1
fi

cat testdir/${file}
if test $? == 0; then
	echo "ERROR: Should have failed to read file ${file}. Mode is now 600. Only root can read."
	exit 1
fi

sudo cat testdir/${file}
if test $? != 0; then
	echo "ERROR: Failed to read file ${file}, Directory read/write DAC override failed for directory"
	exit 1
fi
