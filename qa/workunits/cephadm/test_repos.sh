#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})
CEPHADM_SRC_DIR=${SCRIPT_DIR}/../../../src/cephadm

[ -d "$TMPDIR" ] || TMPDIR=$(mktemp -d tmp.$SCRIPT_NAME.XXXXXX)
trap "$SUDO rm -rf $TMPDIR" EXIT

if [ -z "$CEPHADM" ]; then
    CEPHADM_RELATIVE_PATH=`mktemp -p $TMPDIR tmp.cephadm.XXXXXX`
    # make sure to get the absolute path to avoid any
    # potential issues with jinja2 when using relative paths
    CEPHADM=`realpath $CEPHADM`
    ${CEPHADM_SRC_DIR}/build.sh "$CEPHADM"
fi

# this is a pretty weak test, unfortunately, since the
# package may also be in the base OS.
function test_install_uninstall() {
    ( sudo apt update && \
	  sudo apt -y install cephadm && \
	  sudo $CEPHADM install && \
	  sudo apt -y remove cephadm ) || \
	( sudo yum -y install cephadm && \
	      sudo $CEPHADM install && \
	      sudo yum -y remove cephadm ) || \
	( sudo dnf -y install cephadm && \
	      sudo $CEPHADM install && \
	      sudo dnf -y remove cephadm ) || \
	( sudo zypper -n install cephadm && \
	      sudo $CEPHADM install && \
	      sudo zypper -n remove cephadm )
}

sudo $CEPHADM -v add-repo --release octopus
test_install_uninstall
sudo $CEPHADM -v rm-repo

sudo $CEPHADM -v add-repo --dev main
test_install_uninstall
sudo $CEPHADM -v rm-repo

sudo $CEPHADM -v add-repo --release 15.2.7
test_install_uninstall
sudo $CEPHADM -v rm-repo

echo OK.
