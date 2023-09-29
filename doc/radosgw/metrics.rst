==============
RGW Op Metrics
==============

The Radosgw tracks internal metrics related to S3 operations. These metrics can be sent to Prometheus to see a cluster wide view of usage data (ex: number of S3 put operations on a bucket) over time.

Under the hood, these internal metrics are :ref:`Labeled Perf Counters` and these counters are stored in a two caches. One cache for counters labeled by Radosgw user. The other cache for counters labeled by bucket name.

.. contents::

Current Radosgw Op Metrics
==========================

The following metrics related to S3 operations are tracked per Radosgw.

.. list-table:: Radosgw Op Metrics
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

Enabling the user or bucket counter caches also tracks the above op metrics per user or bucket.

For example is the bucket counter cache is enabled and a bucket named ``Bucket1`` has been created, the Radosgw op metrics specific to ``Bucket1``, can be seen in the ``rgw_op`` section of the output of the ``counter dump`` command.::

    "rgw_op": [
        ...
        {
            "labels": {
                "Bucket": "Bucket1",
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

Enabling the Radosgw User & Bucket Counter Caches
=================================================

To track op metrics by user the Radosgw the config value ``rgw_user_counters_cache`` must be set to ``true``. 

To track op metrics by bucket the Radosgw the config value ``rgw_bucket_counters_cache`` must be set to ``true``. 

These config values are set in the Ceph configuration file under the ``[client.rgw.{instance-name}]`` section and must be set before the radosgw is started or restarted to take effect.

The counters in these caches live in memory. If the radosgw is restarted or crashes, all the  counters in the cache are lost.

Radosgw User & Bucket Counter Cache Size & Eviction
===================================================

The user and bucket counter cache sizes should be sized appropriately depending on the expected number of users and buckets in the cluster.

To help calculate the memory usage of a cache, it should be noted that each cache entry, encompassing all of the op metrics, is 1360 bytes. Both ``rgw_user_counters_cache_size`` and ``rgw_bucket_counters_cache_size`` can be used to set number of entries in each cache.

Counters are evicted from a cache once the number of counters in the cache are greater than the cache size config variable. The counters that are evicted are the least recently used (LRU). 

For example if the number of buckets exceeded ``rgw_bucket_counters_cache_size`` by 1 and the counters with label ``bucket1`` were the last to be updated, the counters for ``bucket1`` would be evicted from the cache. If S3 operations tracked by the op metrics were done on ``bucket1`` after eviction, all of the metrics in the cache for ``bucket1`` would start at 0.

Sending Metrics to Prometheus
=============================

To get metrics from a Radosgw into the time series database Prometheus, the ceph-exporter daemon must be running and configured to scrape the Radogw's admin socket.::

  ceph-exporter --stats-period=5 --sock-dir=/dir/to/radosgw/asok/file

The ceph-exporter daemon scrapes the radosgw's admin socket at a regular interval, defined by the config variable ``exporter_stats_period``.

Prometheus has a configurable interval in which it scrapes the exporter (see: https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

If counters are being evicted from the cache, aligning scraping intervals is imperative to ensure all metrics end up in Prometheus.

Config Reference
================
The following rgw op metrics related settings can be added to the Ceph configuration file
(i.e., usually `ceph.conf`) under the ``[client.rgw.{instance-name}]`` section.

.. confval:: rgw_user_counters_cache
.. confval:: rgw_user_counters_cache_size
.. confval:: rgw_bucket_counters_cache
.. confval:: rgw_bucket_counters_cache_size

The following are notable ceph-exporter related settings that can be added under the ``[global]`` section of the Ceph configuration file.

.. confval:: exporter_stats_period
