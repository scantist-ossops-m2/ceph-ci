===========
RGW Metrics
===========

The Radosgw can use labeled perf counters to keep track of certain labeled internal metrics such as the number or S3 put operations on a bucket by a user. These internal metrics can be accessed via the admin socket (see :ref:`Labeled Perf Counters`) or sent to Prometheus via the Ceph exporter daemon. 

These metrics are stored in a cache that holds perf counters. Once enabled, each Radosgw has a labeled perf counters cache. Perf counters are evicted once the number of labeled perf counters in the cache are greater than ``rgw_perf_counters_cache_size``.

.. contents::

Enabling the Radosgw Perf Counter Cache
=======================================

To enable the perf counters cache of a radosgw the config value ``rgw_perf_counters_cache`` must be set to ``true``. This config value is set in the Ceph configuration file under the ``[client.rgw.{instance-name}]`` section and must be set before the radosgw is started or restarted.

The perf counters live in memory. If the radosgw is restarted or crashes, all the perf counters in the cache are lost.

Radosgw Perf Counter Cache Evictions
=====================================

Perf counters are evicted once the number of labeled perf counters in the cache are greater than ``rgw_perf_counters_cache_size``. The labeled counters that are evicted are the least recently used (LRU).

Running the Ceph Exporter
=========================

To get metrics into the time series database Prometheus, the ceph-exporter daemon must be running and configured to scrape a radogw's admin socket.::

  ceph-exporter --sock-dir /dir/to/radosgw/asok/file

Current Radosgw Metrics
=======================

When the cache is enabled the following metrics are tracked for common S3 operations.
Each of these metrics are emitted with labels for the user that did the operation and (if applicable) the bucket the operation is done on.

.. list-table:: RGW Op Metrics
   :widths: 50

   * - Put Ops
   * - Put Bytes
   * - Put Latency
   * - Get Ops
   * - Get Bytes
   * - Get Latency
   * - Delete Object Ops
   * - Delete Object Bytes
   * - Delete Object Latency
   * - Delete Bucket Ops
   * - Delete Bucket Bytes
   * - Delete Bucket Latency
   * - Copy Object Ops
   * - Copy Object Bytes
   * - Copy Object Latency
   * - List Object Ops
   * - List Object Latency
   * - List Bucket Ops
   * - List Bucket Latency

For a user ``User1`` doing operations on ``Bucket1``, the RGW Op Metrics can be seen in the ``rgw_op`` section of the output of the ``counter dump`` command.::

    "rgw_op": [
        ...
        {
            "labels": {
                "Bucket": "Bucket1",
                "User": "User1"
            },
            "counters": {
                "put_ops": 2,
                "put_b": 5327,
                "put_initial_lat": {
                    "avgcount": 2,
                    "sum": 2.818064835,
                    "avgtime": 1.409032417
                },
                "get_ops": 5,
                "get_b": 5325,
                "get_initial_lat": {
                    "avgcount": 2,
                    "sum": 0.003000069,
                    "avgtime": 0.001500034
                },
                "del_obj_ops": 2,
                "del_obj_bytes": 5327,
                "del_obj_lat": {
                    "avgcount": 2,
                    "sum": 0.034000782,
                    "avgtime": 0.017000391
                },
                "del_bucket_ops": 1,
                "del_bucket_lat": {
                    "avgcount": 1,
                    "sum": 0.134003083,
                    "avgtime": 0.134003083
                },
                "copy_obj_ops": 1,
                "copy_obj_bytes": 5033,
                "copy_obj_lat": {
                    "avgcount": 1,
                    "sum": 0.024000553,
                    "avgtime": 0.024000553
                },
                "list_obj_ops": 1,
                "list_obj_lat": {
                    "avgcount": 1,
                    "sum": 0.004000092,
                    "avgtime": 0.004000092
                },
                "list_buckets_ops": 1,
                "list_buckets_lat": {
                    "avgcount": 1,
                    "sum": 0.002300000,
                    "avgtime": 0.002300000
                }
            }
        },
        ...
    ]


Config Reference
================
The following rgw metrics related settings can be added to the Ceph configuration file
(i.e., usually `ceph.conf`) under the ``[client.rgw.{instance-name}]`` section.

.. confval:: rgw_perf_counters_cache
.. confval:: rgw_perf_counters_cache_size
