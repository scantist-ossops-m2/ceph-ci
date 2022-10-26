from logging import getLogger

from io import StringIO
from tasks.cephfs.xfstests_dev import XFSTestsDev


log = getLogger(__name__)


class TestFscrypt(XFSTestsDev):

    def test_fscrypt(self):
        from tasks.cephfs.fuse_mount import FuseMount
        from tasks.cephfs.kernel_mount import KernelMount

        # TODO: make xfstests-dev compatible with ceph-fuse. xfstests-dev
        # remounts CephFS before running tests using kernel, so ceph-fuse
        # mounts are never actually testsed.
        if isinstance(self.mount_a, FuseMount):
            log.info('client is fuse mounted')
            self.skipTest('Requires kernel client; xfstests-dev not '\
                          'compatible with ceph-fuse ATM.')
        elif isinstance(self.mount_a, KernelMount):
            log.info('client is kernel mounted')

        # XXX: check_status is set to False so that we can check for command's
        # failure on our own (since this command doesn't set right error code
        # and error message in some cases) and print custom log messages
        # accordingly.
        proc = self.mount_a.client_remote.run(args=['sudo', './check',
            '-g', 'encrypt'], cwd=self.repo_path, stdout=StringIO(),
            stderr=StringIO(), timeout=900, check_status=False,omit_sudo=False,
            label='running tests for encrypt from xfstests-dev')

        if proc.returncode != 0:
            log.info('Command failed.')
        log.info(f'Command return value: {proc.returncode}')
        stdout, stderr = proc.stdout.getvalue(), proc.stderr.getvalue()
        log.info(f'Command stdout -\n{stdout}')
        log.info(f'Command stderr -\n{stderr}')

        # Currently only 395,396,397,421,429,435,440,580,593,595,598 of
        # the will 26 test cases will pass, all the others will be skipped
        # because of not supported features in kernel or kernel ceph.
        self.assertEqual(proc.returncode, 0)
        success_line = 'Passed all 26 tests'
        self.assertIn(success_line, stdout)
