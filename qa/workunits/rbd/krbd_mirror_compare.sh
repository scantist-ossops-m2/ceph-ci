#!/bin/bash

set -ex
CLUSTER1=cluster1
CLUSTER2=cluster2

BIN_PATH=''

launch_cluster () {
  local cluster=$1

  MON=1 OSD=1 MGR=1 MDS=0 RGW=0 ../src/mstart.sh $cluster --short -n -d -o "
  mirroring_debug_snap_copy_delay = 9876
  " --without-dashboard
}

setup () {
  local cluster1=$1
  local cluster2=$2
  local pool=$3
  local image=$4
  ##removed, see launch_manual_msnaps  local interval=$5

  #setup environment
  #launch_cluster $cluster1
  #launch_cluster $cluster2

  ${BIN_PATH}ceph --cluster $cluster1 osd pool create $pool
  ${BIN_PATH}rbd --cluster $cluster1 pool init $pool

  ${BIN_PATH}ceph --cluster $cluster2 osd pool create $pool
  ${BIN_PATH}rbd --cluster $cluster2 pool init $pool

  ${BIN_PATH}rbd --cluster $cluster1 mirror pool enable $pool image
  
  ${BIN_PATH}rbd --cluster $cluster1 mirror pool peer bootstrap create $pool | tail -n 1 > token
  ${BIN_PATH}rbd --cluster $cluster2 mirror pool peer bootstrap import --site-name $cluster2 $pool token

  ${BIN_PATH}rbd-mirror --cluster $cluster1 --rbd-mirror-delete-retry-interval=5 --rbd-mirror-image-state-check-interval=5   --rbd-mirror-journal-poll-age=1 --rbd-mirror-pool-replayers-refresh-interval=5 --daemonize=true
  ${BIN_PATH}rbd-mirror --cluster $cluster2 --rbd-mirror-delete-retry-interval=5 --rbd-mirror-image-state-check-interval=5   --rbd-mirror-journal-poll-age=1 --rbd-mirror-pool-replayers-refresh-interval=5 --daemonize=true

  #create image
  ${BIN_PATH}rbd --cluster $cluster1 create $image --size 10G --pool $pool  --image-feature layering,exclusive-lock,object-map,fast-diff 

  #setup snapshot mirroring
  ${BIN_PATH}rbd --cluster $cluster1 mirror image enable $pool/$image snapshot

  #set schedule for image
#  ${BIN_PATH}rbd mirror snapshot schedule add --pool $pool --image $image $interval  --cluster $cluster1
}

map () {
  local cluster=$1
  local pool=$2
  local image=$3
  local snap=$4

  if [[ -n $snap ]]; then
    snap="@$snap"
  fi

  sudo ${BIN_PATH}rbd --cluster $cluster device map $pool/$image$snap
}

format () {
  local bdev=$1
  
  sudo mkfs.ext4 $bdev
}

mount() {
  local bdev=$1
  local mountpt=$2
  local user=$3

  sudo mkdir -p $mountpt
  sudo mount $bdev $mountpt

  sudo chown $user $mountpt
}

launch_manual_msnaps() {
  local cluster=$1
  local pool=$2
  local image=$3

  for i in {1..10}; do
    ${BIN_PATH}rbd --cluster $cluster mirror image snapshot $pool/$image --debug-rbd 0;
    sleep 3s;
  done

  sleep 10s;
  killall timeout

}

run_bench() {
  local timeout=$1

  KERNEL_TAR_URL="https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-4.14.280.tar.gz"
  wget $KERNEL_TAR_URL -O /mnt/test/kernel.tar.gz
  timeout $timeout bash -c "tar xvfz /mnt/test/kernel.tar.gz -C /mnt/test/ | pv -L 1k --timer &> /dev/null" || true
}

umount () {
  local mountpt=$1
  
  sudo umount $mountpt
}

unmap () {
  local cluster=$1
  local pool=$2
  local image=$3
  local snap=$4
  
  if [[ -n $snap ]]; then
    snap="@$snap"
  fi

  sudo ${BIN_PATH}rbd --cluster $cluster device unmap $pool/$image$snap
}

promote () {
  local cluster=$1
  local pool=$2
  local image=$3

  sudo ${BIN_PATH}rbd --cluster $cluster mirror image promote $pool/$image
}

demote () {
  local cluster=$1
  local pool=$2
  local image=$3

  sudo ${BIN_PATH}rbd --cluster $cluster mirror image demote $pool/$image
}

wait_for_demote_snap () {
  local cluster=$1
  local pool=$2
  local image=$3

  while [ true ] ;
  do
    RET=`${BIN_PATH}rbd --cluster $cluster snap ls --all $pool/$image | grep non_primary | grep demote | grep -v "%" ||true`
    if [ "$RET" != "" ]; then
      echo demoted snapshot received, continuing
      sleep 10s #wait a bit for it to propagate
      break
    fi

    echo waiting for demoted snapshot...
    sleep 5s
  done
}

fsck_check () {
  local bdev=$1

  sudo fsck -fn $bdev
}

CLUSTER1=site-a
CLUSTER2=site-b
POOL=pool1
IMAGE=image1
MOUNT=/mnt/test
PRIMARY=${CLUSTER1}
SECONDARY=${CLUSTER2}
MIRROR_INTERVAL=1m
WORKLOAD_TIMEOUT=5m
MY_USER=$(whoami)

setup ${PRIMARY} ${SECONDARY} ${POOL} ${IMAGE} ${MIRROR_INTERVAL}

#initial setup
BDEV=$(map ${PRIMARY} ${POOL} ${IMAGE})
format ${BDEV}

mount ${BDEV} ${MOUNT} ${MY_USER}

launch_manual_msnaps ${PRIMARY} ${POOL} ${IMAGE} &
run_bench ${WORKLOAD_TIMEOUT}
sync
umount ${MOUNT}
unmap ${PRIMARY} ${POOL} ${IMAGE}
demote ${PRIMARY} ${POOL} ${IMAGE}

DEMOTE=$(${BIN_PATH}rbd --cluster site-a snap ls --all pool1/image1 --debug-rbd 0 | grep mirror\.primary | grep demoted | awk '{print $2}')
BDEV=$(map ${PRIMARY} ${POOL} ${IMAGE} ${DEMOTE})
DEMOTE_MD5=$(sudo dd if=${BDEV} bs=4M | md5sum | awk '{print $1}')
unmap ${PRIMARY} ${POOL} ${IMAGE} ${DEMOTE}
promote ${SECONDARY} ${POOL} ${IMAGE}

TEMP=${PRIMARY}
PRIMARY=${SECONDARY}
SECONDARY=${TEMP}

PROMOTE=$(${BIN_PATH}rbd --cluster site-b snap ls --all pool1/image1 --debug-rbd 0 | grep mirror\.primary | awk '{print $2}')
BDEV=$(map ${PRIMARY} ${POOL} ${IMAGE} ${PROMOTE})
PROMOTE_MD5=$(sudo dd if=${BDEV} bs=4M | md5sum | awk '{print $1}')
  
unmap ${PRIMARY} ${POOL} ${IMAGE} ${PROMOTE}
[ "${DEMOTE_MD5}" == "${PROMOTE_MD5}" ]
