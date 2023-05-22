import logging
from .baseobjectstore import BaseObjectStore
from ceph_volume.util import system

logger = logging.getLogger(__name__)


class BlueStore(BaseObjectStore):
    def __init__(self, args):
        super().__init__(args)
        self.args = args
        self.objectstore = 'bluestore'
        self.osd_id = None
        self.osd_fsid = ''
        self.key = None
        self.block_device_path = ''
        self.wal_device_path = ''
        self.db_device_path = ''

    def add_objectstore_opts(self):
        """
        Create the files for the OSD to function. A normal call will look like:

            ceph-osd --cluster ceph --mkfs --mkkey -i 0 \
                    --monmap /var/lib/ceph/osd/ceph-0/activate.monmap \
                    --osd-data /var/lib/ceph/osd/ceph-0 \
                    --osd-uuid 8d208665-89ae-4733-8888-5d3bfbeeec6c \
                    --keyring /var/lib/ceph/osd/ceph-0/keyring \
                    --setuser ceph --setgroup ceph

        In some cases it is required to use the keyring, when it is passed in as
        a keyword argument it is used as part of the ceph-osd command
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
            self.osd_mkfs_cmd.extend(['--osdspec-affinity', self.get_osdspec_affinity()])
