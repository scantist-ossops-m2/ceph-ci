import json
import logging
from ceph_volume import conf, terminal, decorators
from ceph_volume.api import lvm as api
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.util import system, disk
from ceph_volume.devices.lvm.common import rollback_osd
from .bluestore import BlueStore

logger = logging.getLogger(__name__)

class LvmBlueStore(BlueStore):
    def __init__(self, args):
        super().__init__(args)
        self.tags = {}
        self.block_lv = None

    def pre_prepare(self):
        if self.encrypted:
            self.secrets['dmcrypt_key'] = encryption_utils.create_dmcrypt_key()

        cluster_fsid = self.get_cluster_fsid()

        self.osd_fsid = self.args.osd_fsid or system.generate_uuid()
        crush_device_class = self.args.crush_device_class
        if crush_device_class:
            self.secrets['crush_device_class'] = crush_device_class
        # reuse a given ID if it exists, otherwise create a new ID
        self.osd_id = prepare_utils.create_id(self.osd_fsid, json.dumps(self.secrets), osd_id=self.args.osd_id)
        self.tags = {
            'ceph.osd_fsid': self.osd_fsid,
            'ceph.osd_id': self.osd_id,
            'ceph.cluster_fsid': cluster_fsid,
            'ceph.cluster_name': conf.cluster,
            'ceph.crush_device_class': crush_device_class,
            'ceph.osdspec_affinity': self.get_osdspec_affinity()
        }

        try:
            vg_name, lv_name = self.args.data.split('/')
            self.block_lv = api.get_single_lv(filters={'lv_name': lv_name,
                                                    'vg_name': vg_name})
        except ValueError:
            self.block_lv = None

        if not self.block_lv:
            self.block_lv = self.prepare_data_device('block', self.osd_fsid)
        self.block_device_path = self.block_lv.lv_path

        self.tags['ceph.block_device'] = self.block_lv.lv_path
        self.tags['ceph.block_uuid'] = self.block_lv.lv_uuid
        self.tags['ceph.cephx_lockbox_secret'] = self.cephx_lockbox_secret
        self.tags['ceph.encrypted'] = self.encrypted
        self.tags['ceph.vdo'] = api.is_vdo(self.block_lv.lv_path)

    def prepare_data_device(self, device_type, osd_uuid):
        """
        Check if ``arg`` is a device or partition to create an LV out of it
        with a distinct volume group name, assigning LV tags on it and
        ultimately, returning the logical volume object.  Failing to detect
        a device or partition will result in error.

        :param arg: The value of ``--data`` when parsing args
        :param device_type: Usually ``block``
        :param osd_uuid: The OSD uuid
        """

        device = self.args.data
        if disk.is_partition(device) or disk.is_device(device):
            # we must create a vg, and then a single lv
            lv_name_prefix = "osd-{}".format(device_type)
            kwargs = {'device': device,
                      'tags': {'ceph.type': device_type},
                      'slots': self.args.data_slots,
                     }
            logger.debug('data device size: {}'.format(self.args.data_size))
            if self.args.data_size != 0:
                kwargs['size'] = self.args.data_size
            return api.create_lv(
                lv_name_prefix,
                osd_uuid,
                **kwargs)
        else:
            error = [
                'Cannot use device ({}).'.format(device),
                'A vg/lv path or an existing device is needed']
            raise RuntimeError(' '.join(error))

        raise RuntimeError('no data logical volume found with: {}'.format(device))

    def safe_prepare(self, args=None):
        """
        An intermediate step between `main()` and `prepare()` so that we can
        capture the `self.osd_id` in case we need to rollback

        :param args: Injected args, usually from `lvm create` which compounds
                     both `prepare` and `create`
        """
        if args is not None:
            self.args = args

        try:
            vgname, lvname = self.args.data.split('/')
            lv = api.get_single_lv(filters={'lv_name': lvname,
                                            'vg_name': vgname})
        except ValueError:
            lv = None

        if api.is_ceph_device(lv):
            logger.info("device {} is already used".format(self.args.data))
            raise RuntimeError("skipping {}, it is already prepared".format(self.args.data))
        try:
            self.prepare()
        except Exception:
            logger.exception('lvm prepare was unable to complete')
            logger.info('will rollback OSD ID creation')
            rollback_osd(self.args, self.osd_id)
            raise
        terminal.success("ceph-volume lvm prepare successful for: %s" % self.args.data)

    @decorators.needs_root
    def prepare(self):
        # 1/
        self.pre_prepare() # Need to be reworked (move it to the parent class + call super()? )

        # 2/
        self.wal_device_path, wal_uuid, tags = self.setup_device(
            'wal',
            self.args.block_wal,
            self.tags,
            self.args.block_wal_size,
            self.args.block_wal_slots)
        self.db_device_path, db_uuid, tags = self.setup_device(
            'db',
            self.args.block_db,
            self.tags,
            self.args.block_db_size,
            self.args.block_db_slots)

        self.tags['ceph.type'] = 'block'
        self.block_lv.set_tags(self.tags)

        # 3/ encryption-only operations
        if self.secrets.get('dmcrypt_key'):
            self.prepare_dmcrypt()

        # 4/ osd_prepare req
        self.prepare_osd_req()

        # 5/ bluestore mkfs
        # prepare the osd filesystem
        self.osd_mkfs()

    def prepare_dmcrypt(self):
        # If encrypted, there is no need to create the lockbox keyring file because
        # bluestore re-creates the files and does not have support for other files
        # like the custom lockbox one. This will need to be done on activation.
        # format and open ('decrypt' devices) and re-assign the device and journal
        # variables so that the rest of the process can use the mapper paths
        key = self.secrets['dmcrypt_key']
        self.block_device_path = self.luks_format_and_open(key, self.block_device_path, 'block', self.tags)
        self.wal_device_path = self.luks_format_and_open(key, self.wal_device_path, 'wal', self.tags)
        self.db_device_path = self.luks_format_and_open(key, self.db_device_path, 'db', self.tags)

    def luks_format_and_open(self, key, device, device_type, tags):
        """
        Helper for devices that are encrypted. The operations needed for
        block, db, wal devices are all the same
        """
        if not device:
            return ''
        tag_name = 'ceph.%s_uuid' % device_type
        uuid = tags[tag_name]
        # format data device
        encryption_utils.luks_format(
            key,
            device
        )
        encryption_utils.luks_open(
            key,
            device,
            uuid
        )

        return '/dev/mapper/%s' % uuid

    def get_cluster_fsid(self):
        """
        Allows using --cluster-fsid as an argument, but can fallback to reading
        from ceph.conf if that is unset (the default behavior).
        """
        if self.args.cluster_fsid:
            return self.args.cluster_fsid
        else:
            return conf.ceph.get('global', 'fsid')

    def setup_device(self, device_type, device_name, tags, size, slots):
        """
        Check if ``device`` is an lv, if so, set the tags, making sure to
        update the tags with the lv_uuid and lv_path which the incoming tags
        will not have.

        If the device is not a logical volume, then retrieve the partition UUID
        by querying ``blkid``
        """
        if device_name is None:
            return '', '', tags
        tags['ceph.type'] = device_type
        tags['ceph.vdo'] = api.is_vdo(device_name)

        try:
            vg_name, lv_name = device_name.split('/')
            lv = api.get_single_lv(filters={'lv_name': lv_name,
                                            'vg_name': vg_name})
        except ValueError:
            lv = None

        if lv:
            lv_uuid = lv.lv_uuid
            path = lv.lv_path
            tags['ceph.%s_uuid' % device_type] = lv_uuid
            tags['ceph.%s_device' % device_type] = path
            lv.set_tags(tags)
        elif disk.is_device(device_name):
            # We got a disk, create an lv
            lv_type = "osd-{}".format(device_type)
            name_uuid = system.generate_uuid()
            kwargs = {
                'device': device_name,
                'tags': tags,
                'slots': slots
            }
            #TODO use get_block_db_size and co here to get configured size in
            #conf file
            if size != 0:
                kwargs['size'] = size
            lv = api.create_lv(
                lv_type,
                name_uuid,
                **kwargs)
            path = lv.lv_path
            tags['ceph.{}_device'.format(device_type)] = path
            tags['ceph.{}_uuid'.format(device_type)] = lv.lv_uuid
            lv_uuid = lv.lv_uuid
            lv.set_tags(tags)
        else:
            # otherwise assume this is a regular disk partition
            name_uuid = self.get_ptuuid(device_name)
            path = device_name
            tags['ceph.%s_uuid' % device_type] = name_uuid
            tags['ceph.%s_device' % device_type] = path
            lv_uuid = name_uuid
        return path, lv_uuid, tags