#!/bin/bash

set -ex

sudo modprobe nvme-fabrics
sudo modprobe nvme-tcp

# import NVMEOF_GATEWAY_IP_ADDRESS and NVMEOF_GATEWAY_NAME
source /etc/ceph/nvmeof.env

GATEWAY=$(echo $NVMEOF_GATEWAY_NAME)
IP=$(echo $NVMEOF_GATEWAY_IP_ADDRESS)
HOSTNAME=$(hostname)
IMAGE="quay.io/ceph/nvmeof-cli:latest"
POOL="mypool"
MIMAGE="myimage"
BDEV="mybdev"
SERIAL="SPDK00000000000001"
NQN="nqn.2016-06.io.spdk:cnode1"
PORT="4420"
SRPORT="5500"
DISCOVERY_PORT="8009"

sudo podman run -it $IMAGE --server-address $IP --server-port $SRPORT create_bdev --pool $POOL --image $MIMAGE --bdev $BDEV
sudo podman images
sudo podman ps
sudo podman run -it $IMAGE --server-address $IP --server-port $SRPORT create_subsystem --subnqn $NQN --serial $SERIAL
sudo podman run -it $IMAGE --server-address $IP --server-port $SRPORT add_namespace --subnqn $NQN --bdev $BDEV
sudo podman run -it $IMAGE --server-address $IP --server-port $SRPORT create_listener -n $NQN -g client.$GATEWAY -a $IP -s $PORT
sudo podman run -it $IMAGE --server-address $IP --server-port $SRPORT add_host --subnqn $NQN --host "*"
sudo podman run -it $IMAGE --server-address $IP --server-port $SRPORT get_subsystems
sudo lsmod | grep nvme
sudo nvme list
sudo nvme discover -t tcp -a $IP -s $DISCOVERY_PORT
sudo nvme connect -t tcp --traddr $IP -s $PORT -n $NQN
sudo nvme list

echo "testing nvmeof initiator..."

if ! sudo nvme list | grep -q "SPDK bdev Controller"; then
  echo "nvmeof initiator not created!"
  exit 1
fi

echo "nvmeof initiator created: success!"