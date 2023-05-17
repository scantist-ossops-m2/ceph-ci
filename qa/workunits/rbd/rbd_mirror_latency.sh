#!/bin/bash

set -ex

function setup_ns() {
  local namespace1=$1
  local ip_address1=$2
  local namespace2=$3
  local ip_address2=$4

  sudo ip netns add "$namespace1"
  sudo ip netns add "$namespace2"
  sudo ip link add ptp-"$namespace1" type veth peer name ptp-"$namespace2"

  sudo ip link set ptp-"$namespace1" netns "$namespace1"
  sudo ip link set ptp-"$namespace2" netns "$namespace2"

  sudo ip netns exec "$namespace1" ip addr add "$ip_address1" dev ptp-"$namespace1"
  sudo ip netns exec "$namespace1" ip link set dev ptp-"$namespace1" up

  sudo ip netns exec "$namespace1" ip addr add 127.0.0.1 dev lo
  sudo ip netns exec "$namespace1" ip link set dev lo up

  sudo ip netns exec "$namespace2" ip addr add "$ip_address2" dev ptp-"$namespace2"
  sudo ip netns exec "$namespace2" ip link set dev ptp-"$namespace2" up

  sudo ip netns exec "$namespace2" ip addr add 127.0.0.1 dev lo
  sudo ip netns exec "$namespace2" ip link set dev lo up
}

function destroy_ns() {
  local namespace=$1

  sudo ip netns del "$namespace"
}

function launch_cluster() {
  local namespace=$1

  sudo ip netns exec "$namespace" bash -c "MON=1 OSD=1 MGR=1 MDS=0 RGW=0 ../src/mstart.sh $namespace --short -n -d --without-dashboard"
}

function config_cluster() {
  local namespace=$1
  local designation=$2
  CEPH_CONF=run/$namespace/ceph.conf

  ln -s run/"$namespace"/ceph.conf "$namespace".conf

  sudo ip netns exec "$namespace" ./bin/ceph -c "$CEPH_CONF" osd pool create pool1
  sudo ip netns exec "$namespace" ./bin/rbd -c "$CEPH_CONF" pool init pool1

  if [ "$designation" == "primary" ] ; then
    sudo ip netns exec "$namespace" ./bin/rbd -c "$CEPH_CONF" mirror pool enable pool1 image
  
    #create token
    sudo ip netns exec "$namespace" ./bin/rbd -c "$CEPH_CONF" mirror pool peer bootstrap create pool1 | tail -n 1 > token
  
    sudo ip netns exec "$namespace" ./bin/rbd-mirror --cluster "$namespace" --rbd-mirror-delete-retry-interval=5 --rbd-mirror-image-state-check-interval=5 --rbd-mirror-journal-poll-age=1 --rbd-mirror-pool-replayers-refresh-interval=5 --debug-rbd=40 --debug-journaler=40 --debug-rbd_mirror=40 --daemonize=true
  
    sudo ip netns exec "$namespace" ./bin/rbd -c "$CEPH_CONF" create pool1/test-demote-sb --size 100G --image-feature layering --image-feature exclusive-lock --image-feature object-map --image-feature fast-diff --debug-ms 0 --debug-rbd 20 --debug-rbd-mirror 20

    sudo ip netns exec "$namespace" ./bin/rbd -c "$CEPH_CONF" mirror image enable pool1/test-demote-sb snapshot
  fi

  if [ "$designation" == "secondary" ] ; then
    sudo ip netns exec "$namespace" ./bin/rbd -c "$CEPH_CONF" mirror pool peer bootstrap import pool1 token

    sudo ip netns exec "$namespace" ./bin/rbd-mirror --cluster "$namespace" -c "$CEPH_CONF" --rbd-mirror-delete-retry-interval=5 --rbd-mirror-image-state-check-interval=5 --rbd-mirror-journal-poll-age=1 --rbd-mirror-pool-replayers-refresh-interval=5 --debug-rbd=20 --debug-rbd-mirror=20 --daemonize=true
  fi
}

function add_latency() {
  local namespace=$1
  local delay=$2

  sudo ip netns exec "$namespace" tc qdisc add dev ptp-"$namespace" root netem delay "$delay"
}

function remove_latency() {
  local namespace=$1

  sudo ip netns exec "$namespace" tc qdisc del dev ptp-"$namespace" root netem delay 0
}

function launch_io() {
  local namespace=$1
  CEPH_CONF=run/"$namespace"/ceph.conf
  bdev=$(sudo ip netns exec "$namespace" ./bin/rbd -c "$CEPH_CONF" device map -o noudev pool1/test-demote-sb)
  sudo mkfs.xfs "$bdev"
  sudo mkdir -p /mnt/latency_test
  sudo mount "$bdev" /mnt/latency_test
  sudo chmod 777 /mnt/latency_test
  cd /mnt/latency_test || exit

  #output config file
  echo """
[global]
refill_buffers
time_based=1
size=5g
direct=1
group_reporting
ioengine=libaio

[workload]
rw=randrw
rate_iops=40,10
blocksize=4KB
#norandommap
iodepth=4
numjobs=1
runtime=2d""" > smallIO_test

  echo starting fio...
  fio smallIO_test
}

function stop_io() {
  killall -s 9 -w fio
}

function stop_cluster() {
  local namespace=$1

  sudo ip netns exec "$namespace" ../src/mstop.sh "${namespace}"
  rm "$namespace.conf"
}

function env_setup() {
  local namespace1=$1
  local ip_address1=$2
  local namespace2=$3
  local ip_address2=$4

  echo setting up ns...
  setup_ns "$namespace1" "$ip_address1" "$namespace2" "$ip_address2"

  echo launching clusters...
  launch_cluster "$namespace1" 10.10.10.10
  launch_cluster "$namespace2" 10.10.10.20

  echo configuring clusters...
  config_cluster "$namespace1" primary
  config_cluster "$namespace2" secondary  

  echo adding latency...
  add_latency "$namespace1" $LATENCY

}

function create_mirror_snap() {
  local namespace=$1
  
  sudo ip netns exec "$namespace" ./bin/rbd --cluster "$namespace" mirror image snapshot pool1/test-demote-sb
}

function wait_for_new_snap() {
  local namespace=$1

  OUTPUT=$(sudo ip netns exec "$namespace" ./bin/rbd --cluster "$namespace" mirror image status pool1/test-demote-sb | grep bytes_per_second| awk '{print $3}')
  LAST_SNAP_TS=$(echo "$OUTPUT" | jq '.remote_snapshot_timestamp')
  for s in 1 2 4 8 8 8 8 8 8 8 8 16 16 16 16 32 32 32 32; do
    OUTPUT=$(sudo ip netns exec "$namespace" ./bin/rbd --cluster "$namespace" mirror image status pool1/test-demote-sb | grep bytes_per_second| awk '{print $3}')
    REMOTE_TS=$(echo "$OUTPUT" | jq '.remote_snapshot_timestamp')
    if [ "$LAST_SNAP_TS" != "$REMOTE_TS" ] ; then
      return 0
    fi

    echo waiting for new snapshot to appear...
    sleep $s
  done

  return 1
}

function wait_for_snap_sync_complete() {
  local namespace=$1

  for s in 1 2 4 8 8 8 8 8 8 8 8 16 16 16 16 32 32 32 32; do
    OUTPUT=$(sudo ip netns exec "$namespace" ./bin/rbd --cluster "$namespace" mirror image status pool1/test-demote-sb | grep bytes_per_second| awk '{print $3}')
    LOCAL_TS=$(echo "$OUTPUT" | jq '.local_snapshot_timestamp')
    REMOTE_TS=$(echo "$OUTPUT" | jq '.remote_snapshot_timestamp')
    echo "$LOCAL_TS" vs "$REMOTE_TS"

    if [ "$LOCAL_TS" != "" ]  && [ "$LOCAL_TS" == "$REMOTE_TS" ] ; then
      return 0
    fi

    echo waiting for snapshot sync to complete...
    sleep $s
  done

  return 1
}

function tear_down() {
  local namespace1=$1
  local namespace2=$2

  remove_latency "$namespace1"

  sudo umount /mnt/latency_test
  sudo ip netns exec "$namespace1" ./bin/rbd --cluster sitea device unmap -o noudev pool1/test-demote-sb

  stop_cluster "$namespace1"
  stop_cluster "$namespace2"
  sudo killall -s 9 -w rbd-mirror

  destroy_ns "$namespace1"
  destroy_ns "$namespace2"
}

namespace1=sitea
namespace2=siteb
ip_address1="10.10.10.10/24"
ip_address2='10.10.10.20/24'

env_setup "$namespace1" "$ip_address1" "$namespace2" "$ip_address2"

launch_io "$namespace1" &

create_mirror_snap "$namespace1"
wait_for_new_snap "$namespace2"
wait_for_snap_sync_complete "$namespace2"

create_mirror_snap "$namespace1"
wait_for_new_snap "$namespace2"
wait_for_snap_sync_complete "$namespace2"

stop_io
tear_down "$namespace1" "$namespace2"
