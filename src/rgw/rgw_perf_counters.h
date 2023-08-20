// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/common_fwd.h"
#include "common/perf_counters_cache.h"

extern PerfCounters *perfcounter;
extern PerfCountersCache *perf_counters_cache;

extern int rgw_perf_start(CephContext *cct);
extern void rgw_perf_stop(CephContext *cct);

extern std::string_view rgw_op_counters_key;

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

enum {
  l_rgw_labeled_op_first = 16000,

  l_rgw_labeled_put_ops,
  l_rgw_labeled_put_b,
  l_rgw_labeled_put_lat,

  l_rgw_labeled_get_ops,
  l_rgw_labeled_get_b,
  l_rgw_labeled_get_lat,

  l_rgw_labeled_del_obj_ops,
  l_rgw_labeled_del_obj_b,
  l_rgw_labeled_del_obj_lat,

  l_rgw_labeled_del_bucket_ops,
  l_rgw_labeled_del_bucket_lat,

  l_rgw_labeled_copy_obj_ops,
  l_rgw_labeled_copy_obj_b,
  l_rgw_labeled_copy_obj_lat,

  l_rgw_labeled_list_obj_ops,
  l_rgw_labeled_list_obj_lat,

  l_rgw_labeled_list_buckets_ops,
  l_rgw_labeled_list_buckets_lat,

  l_rgw_labeled_op_last
};
