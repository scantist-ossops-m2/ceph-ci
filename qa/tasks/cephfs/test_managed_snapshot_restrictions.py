import errno
import logging
import signal
from textwrap import dedent
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.orchestra.run import Raw
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

MDS_RESTART_GRACE = 60

class TestMonSnapsAndFsPools(CephFSTestCase):
    MDSS_REQUIRED = 3

    def test_disallow_monitor_managed_snaps_for_fs_pools(self):
        """
        Test that creation of monitor managed snaps fails for pools attached
        to any file-system
        """
        with self.assertRaises(CommandFailedError):
            self.fs.rados(["mksnap", "snap1"], pool=self.fs.get_data_pool_name())

        with self.assertRaises(CommandFailedError):
            self.fs.rados(["mksnap", "snap2"], pool=self.fs.get_metadata_pool_name())

    def test_attaching_pools_with_snaps_to_fs_fails(self):
        """
        Test that attempt to attach pool with snapshots to an fs fails
        """
        test_pool_name = 'snap-test-pool'
        base_cmd = f'osd pool create {test_pool_name}'
        ret = self.run_cluster_cmd_result(base_cmd)
        self.assertEqual(ret, 0)

        self.fs.rados(["mksnap", "snap3"], pool=test_pool_name)

        base_cmd = f'fs add_data_pool {self.fs.name} {test_pool_name}'
        ret = self.run_cluster_cmd_result(base_cmd)
        self.assertEqual(ret, errno.EOPNOTSUPP)

        # cleanup
        self.fs.rados(["rmsnap", "snap3"], pool=test_pool_name)
        base_cmd = f'osd pool delete {test_pool_name}'
        ret = self.run_cluster_cmd_result(base_cmd)

    def test_using_pool_with_snap_fails_fs_creation(self):
        """
        Test that using a pool with snaps for fs creation fails
        """
        base_cmd = 'osd pool create test_data_pool'
        ret = self.run_cluster_cmd_result(base_cmd)
        self.assertEqual(ret, 0)
        base_cmd = 'osd pool create test_metadata_pool'
        ret = self.run_cluster_cmd_result(base_cmd)
        self.assertEqual(ret, 0)

        self.fs.rados(["mksnap", "snap4"], pool='test_data_pool')

        base_cmd = 'fs new testfs test_metadata_pool test_data_pool'
        ret = self.run_cluster_cmd_result(base_cmd)
        self.assertEqual(ret, errno.EOPNOTSUPP)

        # cleanup
        self.fs.rados(["rmsnap", "snap4"], pool='test_data_pool')
        base_cmd = 'osd pool delete test_data_pool'
        ret = self.run_cluster_cmd_result(base_cmd)
        base_cmd = 'osd pool delete test_metadata_pool'
        ret = self.run_cluster_cmd_result(base_cmd)
