import logging
from .baseobjectstore import BaseObjectStore
from ceph_volume.util import system
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)


class BlueStore(BaseObjectStore):
    def __init__(self, args: "argparse.Namespace") -> None:
        super().__init__(args)
        self.args: "argparse.Namespace" = args
        self.objectstore = 'bluestore'
        self.osd_id: str = ''
        self.osd_fsid: str = ''
        self.key: Optional[str] = None
        self.block_device_path: str = ''
        self.wal_device_path: str = ''
        self.db_device_path: str = ''

    def add_objectstore_opts(self) -> None:
        """
        Create the files for the OSD to function. A normal call will look like:

            ceph-osd --cluster ceph --mkfs --mkkey -i 0 \
                    --monmap /var/lib/ceph/osd/ceph-0/activate.monmap \
                    --osd-data /var/lib/ceph/osd/ceph-0 \
                    --osd-uuid 8d208665-89ae-4733-8888-5d3bfbeeec6c \
                    --keyring /var/lib/ceph/osd/ceph-0/keyring \
                    --setuser ceph --setgroup ceph

        In some cases it is required to use the keyring, when it is passed
        in as a keyword argument it is used as part of the ceph-osd command
        """

        if self.wal_device_path:
            self.osd_mkfs_cmd.extend(
                ['--bluestore-block-wal-path', self.wal_device_path]
            )
            system.chown(self.wal_device_path)

        if self.db_device_path:
            self.osd_mkfs_cmd.extend(
                ['--bluestore-block-db-path', self.db_device_path]
            )
            system.chown(self.db_device_path)

        if self.get_osdspec_affinity():
            self.osd_mkfs_cmd.extend(['--osdspec-affinity',
                                      self.get_osdspec_affinity()])

        """
        When running in containers the --mkfs on raw device sometimes fails
        to acquire a lock through flock() on the device because systemd-udevd
        holds one temporarily. See KernelDevice.cc and _lock() to understand
        how ceph-osd acquires the lock.
        Because this is really transient, we retry up to 5 times and wait for
        1 sec in-between
        """
