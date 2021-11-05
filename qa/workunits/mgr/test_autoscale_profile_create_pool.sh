#!/bin/sh -ex

PROFILE1=$(ceph osd pool autoscale-status | grep 'device_health_metrics' | grep -oe 'scale-up' || true)

if [[ -z $PROFILE1 ]]
then
  echo "Failure, device_health_metrics PROFILE is empty!"
  exit 1
fi

if [[ $PROFILE1="scale-up" ]]
then
  echo "Success, device_health_metrics PROFILE is scale-up"
  exit 0
else
  echo "Failure, device_health_metrics PROFILE is scale-down"
  exit 1
fi


ceph osd pool create test_pool

sleep 5

PROFILE2=$(ceph osd pool autoscale-status | grep 'test_pool' | grep -oe 'scale-down' || true)

if [[ -z $PROFILE2 ]]
then
  echo "Failure, test_pool PROFILE is empty!"
  exit 1
fi

if [[ $PROFILE2="scale-down" ]]
then
  echo "Success, test_pool PROFILE is scale-down"
  exit 0
else
  echo "Failure, test_pool PROFILE is scale-up"
  exit 1
fi
