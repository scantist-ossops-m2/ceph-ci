from teuthology.contextutil import safe_while, MaxWhileTries
from tasks.cephfs.cephfs_test_case import CephFSTestCase
import logging

log = logging.getLogger(__name__)

class TestDumpExportStates(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1

    EXPORT_STATES = ['locking', 'discovering', 'freezing', 'prepping', 'warning', 'exporting']

    def setUp(self):
        super().setUp()

        self.fs.set_max_mds(self.MDSS_REQUIRED)
        self.status = self.fs.wait_for_daemons()

        self.mount_a.run_shell_payload('mkdir -p test/export')

    def tearDown(self):
        super().tearDown()

    def _wait_for_export_target(self, source, target, sleep=2, timeout=10):
        try:
            with safe_while(sleep=sleep, tries=timeout//sleep) as proceed:
                while proceed():
                    info = self.fs.getinfo().get_rank(self.fs.id, source)
                    log.info(f'waiting for rank {target} to be added to the export target')
                    if target in info['export_targets']:
                        return
        except MaxWhileTries as e:
            raise RuntimeError(f'rank {target} has not been added to export target after {timeout}s') from e

    def _dump_export_state(self, rank):
        states = self.fs.rank_asok(['dump_export_states'], rank=rank, status=self.status)
        self.assertTrue(type(states) is list)
        self.assertEqual(len(states), 1)
        return states[0]

    def _test_base(self, path, source, target, state_index, kill):
        self.fs.rank_asok(['config', 'set', 'mds_kill_import_at', str(kill)], rank=target, status=self.status)

        self.fs.rank_asok(['export', 'dir', path, str(target)], rank=source, status=self.status)
        self._wait_for_export_target(source, target)

        state = self._dump_export_state(source)

        self.assertTrue(type(state['tid']) is int)
        self.assertEqual(state['path'], path)
        self.assertEqual(state['state'], self.EXPORT_STATES[state_index])
        self.assertEqual(state['peer'], target)

        return state

    def _test_state_history(self, state):
        history = state['state_history']
        self.assertTrue(type(history) is dict)
        size = 0
        for name in self.EXPORT_STATES:
            self.assertTrue(type(history[name]) is dict)
            size += 1
            if name == state['state']:
                break
        self.assertEqual(len(history), size)

    def _test_freeze_tree(self, state, waiters):
        self.assertTrue(type(state['freeze_tree_time']) is float)
        self.assertEqual(state['unfreeze_tree_waiters'], waiters)

    def test_discovering(self):
        state = self._test_base('/test', 0, 1, 1, 1)

        self._test_state_history(state)
        self._test_freeze_tree(state, 0)

        self.assertEqual(state['last_cum_auth_pins'], 0)
        self.assertEqual(state['num_remote_waiters'], 0)

    def test_prepping(self):
        client_id = self.mount_a.get_global_id()

        state = self._test_base('/test', 0, 1, 3, 3)

        self._test_state_history(state)
        self._test_freeze_tree(state, 0)

        self.assertEqual(state['flushed_clients'], [client_id])
        self.assertTrue(type(state['warning_ack_waiting']) is list)

    def test_exporting(self):
        state = self._test_base('/test', 0, 1, 5, 5)

        self._test_state_history(state)
        self._test_freeze_tree(state, 0)

        self.assertTrue(type(state['notify_ack_waiting']) is list)
