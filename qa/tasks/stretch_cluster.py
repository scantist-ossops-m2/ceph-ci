import json
import logging
import random
from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestStretchCluster(MgrTestCase):
    """
    Test the stretch cluster feature.
    """
    # Define some constants
    POOL = 'pool_stretch'
    CLUSTER = "ceph"
    WRITE_PERIOD = 10
    RECOVERY_PERIOD = WRITE_PERIOD * 6
    SUCCESS_HOLD_TIME = 7
    # This dictionary maps the datacenter to the osd ids and hosts
    DC_OSDS = {
        'dc1': {
            "node-1": 0,
            "node-2": 1,
            "node-3": 2,
        },
        'dc2': {
            "node-4": 3,
            "node-5": 4,
            "node-6": 5,
        },
        'dc3': {
            "node-7": 6,
            "node-8": 7,
            "node-9": 8,
        }
    }
    PEERING_CRUSH_BUCKET_COUNT = 2
    PEERING_CRUSH_BUCKET_TARGET = 3
    PEERING_CRUSH_BUCKET_BARRIER = 'datacenter'
    CRUSH_RULE = 'replicated_rule_custom'
    SIZE = 6
    MIN_SIZE = 3
    BUCKET_MAX = SIZE // PEERING_CRUSH_BUCKET_TARGET
    if (BUCKET_MAX * PEERING_CRUSH_BUCKET_TARGET) < SIZE:
        BUCKET_MAX += 1

    def setUp(self):
        """
        Setup the cluster and
        ensure we have a clean condition before the test.
        """
        # Ensure we have at least 6 OSDs
        super(TestStretchCluster, self).setUp()
        if self._osd_count() < 6:
            self.skipTest("Not enough OSDS!")

        # Remove any filesystems so that we can remove their pools
        if self.mds_cluster:
            self.mds_cluster.mds_stop()
            self.mds_cluster.mds_fail()
            self.mds_cluster.delete_all_filesystems()

        # Remove all other pools
        for pool in self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']:
            self.mgr_cluster.mon_manager.remove_pool(pool['pool_name'])

    def tearDown(self):
        """
        Clean up the cluter after the test.
        """
        # Remove the pool
        if self.POOL in self.mgr_cluster.mon_manager.pools:
            self.mgr_cluster.mon_manager.remove_pool(self.POOL)

        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for osd in osd_map['osds']:
            # mark all the osd in
            if osd['weight'] == 0.0:
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'in', str(osd['osd']))
            # Bring back all the osds and move it back to the host.
            if osd['up'] == 0:
                self._bring_back_osd(osd['osd'])
                self._move_osd_back_to_host(osd['osd'])
        super(TestStretchCluster, self).tearDown()

    def _setup_pool(self, size=None, min_size=None):
        """
        Create a pool and set its size.
        """
        self.mgr_cluster.mon_manager.create_pool(self.POOL, min_size=min_size)
        if size is not None:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'pool', 'set', self.POOL, 'size', str(size))

    def _osd_count(self):
        """
        Get the number of OSDs in the cluster.
        """
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        return len(osd_map['osds'])

    def _write_some_data(self, t):
        """
        Write some data to the pool to simulate a workload.
        """

        args = [
            "rados", "-p", self.POOL, "bench", str(t), "write", "-t", "16"]

        self.mgr_cluster.admin_remote.run(args=args, wait=True)

    def _kill_osd(self, osd):
        """
        Kill the osd.
        """
        try:
            self.ctx.daemons.get_daemon('osd', osd, self.CLUSTER).stop()
        except Exception:
            log.error("Failed to stop osd.{}".format(str(osd)))
            pass

    def _bring_back_osd(self, osd):
        """
        Bring back the osd.
        """
        try:
            self.ctx.daemons.get_daemon('osd', osd, self.CLUSTER).restart()
        except Exception:
            log.error("Failed to bring back osd.{}".format(str(osd)))
            pass

    def _get_pg_stats(self):
        """
        Dump the cluster and get pg stats
        """
        out = self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'pg', 'dump', '--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        try:
            return j['pg_map']['pg_stats']
        except KeyError:
            return j['pg_stats']

    def _get_active_pg(self, pgs):
        """
        Get the number of active PGs
        """
        num_active = 0
        for pg in pgs:
            if pg['state'].count('active') and not pg['state'].count('stale'):
                num_active += 1
        return num_active

    def _get_active_clean_pg(self, pgs):
        """
        Get the number of active+clean PGs
        """
        num_active_clean = 0
        for pg in pgs:
            if (pg['state'].count('active') and
                pg['state'].count('clean') and
                    not pg['state'].count('stale')):
                num_active_clean += 1
        return num_active_clean

    def _get_acting_set(self, pgs):
        """
        Get the acting set of the PGs
        """
        acting_set = []
        for pg in pgs:
            acting_set.append(pg['acting'])
        return acting_set

    def _surviving_osds_in_acting_set_dont_exceed(self, n, osds):
        """
        Check if the acting set of the PGs doesn't contain more
        than n OSDs of the surviving DC.
        NOTE: Only call this function after we set the pool to stretch.
        """
        pgs = self._get_pg_stats()
        acting_set = self._get_acting_set(pgs)
        for acting in acting_set:
            log.debug("Acting set: %s", acting)
            intersect = list(set(acting) & set(osds))
            if len(intersect) > n:
                log.error(
                    "Acting set: %s contains more than %d \
                    OSDs from the same %s which are: %s",
                    acting, n, self.PEERING_CRUSH_BUCKET_BARRIER,
                    intersect
                )
                return False
        return True

    def _pg_all_active_clean(self):
        """
        Check if all pgs are active and clean.
        """
        pgs = self._get_pg_stats()
        return self._get_active_clean_pg(pgs) == len(pgs)

    def _pg_all_active(self):
        """
        Check if all pgs are active.
        """
        pgs = self._get_pg_stats()
        return self._get_active_pg(pgs) == len(pgs)

    def _pg_all_unavailable(self):
        """
        Check if all pgs are unavailable.
        """
        pgs = self._get_pg_stats()
        return self._get_active_pg(pgs) == 0

    def _pg_partial_active(self):
        """
        Check if some pgs are active.
        """
        pgs = self._get_pg_stats()
        return 0 < self._get_active_pg(pgs) <= len(pgs)

    def _get_osds_by_dc(self, dc):
        """
        Get osds by datacenter.
        """
        return [osd for _, osd in self.DC_OSDS[dc].items()]

    def _get_osds_data(self, want_osds):
        """
        Get the osd data
        """
        all_osds_data = \
            self.mgr_cluster.mon_manager.get_osd_dump_json()['osds']
        return [
            osd_data for osd_data in all_osds_data
            if int(osd_data['osd']) in want_osds
        ]

    def _fail_over_dc(self, dc):
        """
        Fail over the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_OSDS:
            raise ValueError(
                "dc must be one of the following: %s" % self.DC_OSDS.keys()
                )
        osds = self._get_osds_by_dc(dc)
        # fail over all the OSDs in the DC
        for osd_id in osds:
            self._kill_osd(osd_id)
        # wait until all the osds are down
        self.wait_until_true(
            lambda: all([int(osd['up']) == 0
                        for osd in self._get_osds_data(osds)]),
            timeout=self.RECOVERY_PERIOD
        )
        # TODO: also fail over the mons in DC

    def _fail_over_one_osd_from_dc(self, dc):
        """
        Fail over one random OSD from the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_OSDS:
            raise ValueError("dc must be one of the following: %s" %
                             self.DC_OSDS.keys())
        osds = self._get_osds_by_dc(dc)
        # fail over one random OSD in the DC
        osd_id = random.choice(osds)
        self._kill_osd(osd_id)
        # wait until the osd is down
        self.wait_until_true(
            lambda: int(self._get_osds_data([osd_id])[0]['up']) == 0,
            timeout=self.RECOVERY_PERIOD
        )

    def _get_host(self, osd):
        """
        Get the host of the osd.
        """
        for dc, nodes in self.DC_OSDS.items():
            for node, osd_id in nodes.items():
                if osd_id == osd:
                    return node
        return None

    def _move_osd_back_to_host(self, osd):
        """
        Move the osd back to the host.
        """
        host = self._get_host(osd)
        assert host is not None, "The host of osd {} is not found.".format(osd)
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'crush', 'move', 'osd.{}'.format(str(osd)),
            'host={}'.format(host)
        )

    def _bring_back_dc(self, dc):
        """
        Bring back the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_OSDS:
            raise ValueError("dc must be one of the following: %s" %
                             self.DC_OSDS.keys())
        osds = self._get_osds_by_dc(dc)
        # Bring back all the osds in the DC and move it back to the host.
        for osd_id in osds:
            self._bring_back_osd(osd_id)
            self._move_osd_back_to_host(osd_id)

        # wait until all the osds are up
        self.wait_until_true(
            lambda: all([int(osd['up']) == 1
                         for osd in self._get_osds_data(osds)]),
            timeout=self.RECOVERY_PERIOD
        )

    def test_set_stretch_pool_no_active_pgs(self):
        """
        Test setting a pool to stretch cluster and checks whether
        it prevents PGs from the going active when there is not
        enough buckets available in the acting set of PGs to
        go active.
        """
        self._setup_pool(self.SIZE, min_size=self.MIN_SIZE)
        self._write_some_data(self.WRITE_PERIOD)
        # 1. We test the case where we didn't make the pool stretch
        #   and we expect the PGs to go active even when there is only
        #   one bucket available in the acting set of PGs.

        # Fail over DC1 expects PGs to be 100% active
        self._fail_over_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        # Fail over DC2 expects PGs to be partially active
        self._fail_over_dc('dc2')
        self.wait_until_true_and_hold(
            lambda: self._pg_partial_active,
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        # Bring back DC1 expects PGs to be 100% active
        self._bring_back_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # Bring back DC2 expects PGs to be 100% active+clean
        self._bring_back_dc('dc2')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # 2. We test the case where we make the pool stretch
        #   and we expect the PGs to not go active even when there is only
        #   one bucket available in the acting set of PGs.

        # Set the pool to stretch
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'stretch', 'set',
            self.POOL, str(self.PEERING_CRUSH_BUCKET_COUNT),
            str(self.PEERING_CRUSH_BUCKET_TARGET),
            str(self.PEERING_CRUSH_BUCKET_BARRIER),
            self.CRUSH_RULE, str(self.SIZE), str(self.MIN_SIZE))

        # Fail over DC1 expects PGs to be 100% active
        self._fail_over_dc('dc1')
        self.wait_until_true_and_hold(lambda: self._pg_all_active(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # Fail over 1 random OSD from DC2 expects PGs to be 100% active
        self._fail_over_one_osd_from_dc('dc2')
        self.wait_until_true_and_hold(lambda: self._pg_all_active(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # Fail over DC2 completely expects PGs to be 100% inactive
        self._fail_over_dc('dc2')
        self.wait_until_true_and_hold(lambda: self._pg_all_unavailable,
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # We expect that there will be no more than BUCKET_MAX osds from DC3
        # in the acting set of the PGs.
        self.wait_until_true(
            lambda: self._surviving_osds_in_acting_set_dont_exceed(
                        self.BUCKET_MAX,
                        self._get_osds_by_dc('dc3')
                    ),
            timeout=self.RECOVERY_PERIOD)

        # Bring back DC1 expects PGs to be 100% active
        self._bring_back_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME)

        # Bring back DC2 expects PGs to be 100% active+clean
        self._bring_back_dc('dc2')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
