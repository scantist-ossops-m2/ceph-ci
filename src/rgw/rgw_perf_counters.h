// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/common_fwd.h"
#include "common/perf_counters_cache.h"

extern PerfCounters *perfcounter;
extern PerfCountersCache *perf_counters_cache;

extern int rgw_perf_start(CephContext *cct);
extern void rgw_perf_stop(CephContext *cct);
extern void add_rgw_put_counters(PerfCountersBuilder* lpcb);
extern void add_rgw_get_counters(PerfCountersBuilder* lpcb);
extern void add_rgw_del_obj_counters(PerfCountersBuilder* lpcb);
extern void add_rgw_del_bucket_counters(PerfCountersBuilder* lpcb);
extern void add_rgw_copy_obj_counters(PerfCountersBuilder* lpcb);
extern void add_rgw_list_obj_counters(PerfCountersBuilder* lpcb);
extern void add_rgw_list_buckets_counters(PerfCountersBuilder* lpcb);

enum {
  l_rgw_first = 15000,
  l_rgw_req,
  l_rgw_failed_req,

  l_rgw_get,
  l_rgw_get_b,
  l_rgw_get_lat,

  l_rgw_put,
  l_rgw_put_b,
  l_rgw_put_lat,

  l_rgw_qlen,
  l_rgw_qactive,

  l_rgw_cache_hit,
  l_rgw_cache_miss,

  l_rgw_keystone_token_cache_hit,
  l_rgw_keystone_token_cache_miss,

  l_rgw_gc_retire,

  l_rgw_lc_expire_current,
  l_rgw_lc_expire_noncurrent,
  l_rgw_lc_expire_dm,
  l_rgw_lc_transition_current,
  l_rgw_lc_transition_noncurrent,
  l_rgw_lc_abort_mpu,

  l_rgw_pubsub_event_triggered,
  l_rgw_pubsub_event_lost,
  l_rgw_pubsub_store_ok,
  l_rgw_pubsub_store_fail,
  l_rgw_pubsub_events,
  l_rgw_pubsub_push_ok,
  l_rgw_pubsub_push_failed,
  l_rgw_pubsub_push_pending,
  l_rgw_pubsub_missing_conf,

  l_rgw_lua_current_vms,
  l_rgw_lua_script_ok,
  l_rgw_lua_script_fail,

  l_rgw_last,
};

// keys for CountersSetup map in perf counters cache
enum {
  rgw_put_counters = 16000,
  rgw_get_counters,
  rgw_del_obj_counters,
  rgw_del_bucket_counters,
  rgw_copy_obj_counters,
  rgw_list_obj_counters,
  rgw_list_buckets_counters,
};

enum {
  l_rgw_cache_put_first = 16100,
  l_rgw_cache_put_ops,
  l_rgw_cache_put_b,
  l_rgw_cache_put_lat,
  l_rgw_cache_put_last,
};

enum {
  l_rgw_cache_get_first = 16200,
  l_rgw_cache_get_ops,
  l_rgw_cache_get_b,
  l_rgw_cache_get_lat,
  l_rgw_cache_get_last,
};

enum {
  l_rgw_cache_del_obj_first = 16300,
  l_rgw_cache_del_obj_ops,
  l_rgw_cache_del_obj_b,
  l_rgw_cache_del_obj_lat,
  l_rgw_cache_del_obj_last,
};

enum {
  l_rgw_cache_del_bucket_first = 16400,
  l_rgw_cache_del_bucket_ops,
  l_rgw_cache_del_bucket_lat,
  l_rgw_cache_del_bucket_last
};

enum {
  l_rgw_cache_copy_obj_first = 16500,
  l_rgw_cache_copy_obj_ops,
  l_rgw_cache_copy_obj_b,
  l_rgw_cache_copy_obj_lat,
  l_rgw_cache_copy_obj_last
};

enum {
  l_rgw_cache_list_obj_first = 16600,
  l_rgw_cache_list_obj_ops,
  l_rgw_cache_list_obj_lat,
  l_rgw_cache_list_obj_last
};

enum {
  l_rgw_cache_list_buckets_first = 16700,
  l_rgw_cache_list_buckets_ops,
  l_rgw_cache_list_buckets_lat,
  l_rgw_cache_list_buckets_last
};
