#!/usr/bin/env bash
set -ex
podman network create box || true
podman run --rm -i \
  -d \
  --name box_hosts_1 \
  --cap-add SYS_ADMIN,NET_ADMIN,SYS_TIME,SYS_RAWIO,MKNOD,NET_RAW,SETUID,SETGID,CHOWN,SYS_PTRACE,SYS_TTY_CONFIG \
  -v ../../../:/ceph:z \
  -v ..:/cephadm:z \
  -v /run/udev:/run/udev \
  -v /sys/dev/block:/sys/dev/block \
  -v /sys/fs/cgroup:/sys/fs/cgroup \
  -v /dev/fuse:/dev/fuse \
  -v /dev/disk:/dev/disk \
  --device /dev/loop0 \
  --device /dev/loop1 \
  --device /dev/loop2 \
  --security-opt unmask=/sys/dev/block \
  --network box \
  cephadm-box
