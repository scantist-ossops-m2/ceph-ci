// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_perf_counters.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"

PerfCounters *perfcounter = NULL;
PerfCountersCache *perf_counters_cache = NULL;

int rgw_perf_start(CephContext *cct)
{
  PerfCountersBuilder plb(cct, "rgw", l_rgw_first, l_rgw_last);

  // RGW emits comparatively few metrics, so let's be generous
  // and mark them all USEFUL to get transmission to ceph-mgr by default.
  plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  plb.add_u64_counter(l_rgw_req, "req", "Requests");
  plb.add_u64_counter(l_rgw_failed_req, "failed_req", "Aborted requests");

  plb.add_u64_counter(l_rgw_get, "get_ops", "Gets");
  plb.add_u64_counter(l_rgw_get_b, "get_bytes", "Size of gets");
  plb.add_time_avg(l_rgw_get_lat, "get_lat", "Get latency");
  plb.add_u64_counter(l_rgw_put, "put_ops", "Puts");
  plb.add_u64_counter(l_rgw_put_b, "put_bytes", "Size of puts");
  plb.add_time_avg(l_rgw_put_lat, "put_lat", "Put latency");

  plb.add_u64(l_rgw_qlen, "qlen", "Queue length");
  plb.add_u64(l_rgw_qactive, "qactive", "Active requests queue");

  plb.add_u64_counter(l_rgw_cache_hit, "cache_hit", "Cache hits");
  plb.add_u64_counter(l_rgw_cache_miss, "cache_miss", "Cache miss");

  plb.add_u64_counter(l_rgw_keystone_token_cache_hit, "keystone_token_cache_hit", "Keystone token cache hits");
  plb.add_u64_counter(l_rgw_keystone_token_cache_miss, "keystone_token_cache_miss", "Keystone token cache miss");

  plb.add_u64_counter(l_rgw_gc_retire, "gc_retire_object", "GC object retires");

  plb.add_u64_counter(l_rgw_lc_expire_current, "lc_expire_current",
		      "Lifecycle current expiration");
  plb.add_u64_counter(l_rgw_lc_expire_noncurrent, "lc_expire_noncurrent",
		      "Lifecycle non-current expiration");
  plb.add_u64_counter(l_rgw_lc_expire_dm, "lc_expire_dm",
		      "Lifecycle delete-marker expiration");
  plb.add_u64_counter(l_rgw_lc_transition_current, "lc_transition_current",
		      "Lifecycle current transition");
  plb.add_u64_counter(l_rgw_lc_transition_noncurrent,
		      "lc_transition_noncurrent",
		      "Lifecycle non-current transition");
  plb.add_u64_counter(l_rgw_lc_abort_mpu, "lc_abort_mpu",
		      "Lifecycle abort multipart upload");

  plb.add_u64_counter(l_rgw_pubsub_event_triggered, "pubsub_event_triggered", "Pubsub events with at least one topic");
  plb.add_u64_counter(l_rgw_pubsub_event_lost, "pubsub_event_lost", "Pubsub events lost");
  plb.add_u64_counter(l_rgw_pubsub_store_ok, "pubsub_store_ok", "Pubsub events successfully stored");
  plb.add_u64_counter(l_rgw_pubsub_store_fail, "pubsub_store_fail", "Pubsub events failed to be stored");
  plb.add_u64(l_rgw_pubsub_events, "pubsub_events", "Pubsub events in store");
  plb.add_u64_counter(l_rgw_pubsub_push_ok, "pubsub_push_ok", "Pubsub events pushed to an endpoint");
  plb.add_u64_counter(l_rgw_pubsub_push_failed, "pubsub_push_failed", "Pubsub events failed to be pushed to an endpoint");
  plb.add_u64(l_rgw_pubsub_push_pending, "pubsub_push_pending", "Pubsub events pending reply from endpoint");
  plb.add_u64_counter(l_rgw_pubsub_missing_conf, "pubsub_missing_conf", "Pubsub events could not be handled because of missing configuration");
  
  plb.add_u64_counter(l_rgw_lua_script_ok, "lua_script_ok", "Successfull executions of lua scripts");
  plb.add_u64_counter(l_rgw_lua_script_fail, "lua_script_fail", "Failed executions of lua scripts");
  plb.add_u64(l_rgw_lua_current_vms, "lua_current_vms", "Number of Lua VMs currently being executed");
  
  perfcounter = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perfcounter);


  std::function<void(PerfCountersBuilder*)> lpcb_init_puts = add_rgw_put_counters;
  std::function<void(PerfCountersBuilder*)> lpcb_init_gets = add_rgw_get_counters;
  std::function<void(PerfCountersBuilder*)> lpcb_init_del_objs = add_rgw_del_obj_counters;
  std::function<void(PerfCountersBuilder*)> lpcb_init_del_buckets = add_rgw_del_bucket_counters;
  std::function<void(PerfCountersBuilder*)> lpcb_init_copy_objs = add_rgw_copy_obj_counters;
  std::function<void(PerfCountersBuilder*)> lpcb_init_list_objs = add_rgw_list_obj_counters;
  std::function<void(PerfCountersBuilder*)> lpcb_init_list_buckets = add_rgw_list_buckets_counters;

  CountersSetup put_counters_setup(l_rgw_cache_put_first, l_rgw_cache_put_last, lpcb_init_puts);
  CountersSetup get_counters_setup(l_rgw_cache_get_first, l_rgw_cache_get_last, lpcb_init_gets);
  CountersSetup del_obj_counters_setup(l_rgw_cache_del_obj_first, l_rgw_cache_del_obj_last, lpcb_init_del_objs);
  CountersSetup del_bucket_counters_setup(l_rgw_cache_del_bucket_first, l_rgw_cache_del_bucket_last, lpcb_init_del_buckets);
  CountersSetup copy_obj_counters_setup(l_rgw_cache_copy_obj_first, l_rgw_cache_copy_obj_last, lpcb_init_copy_objs);
  CountersSetup list_obj_counters_setup(l_rgw_cache_list_obj_first, l_rgw_cache_list_obj_last, lpcb_init_list_objs);
  CountersSetup list_buckets_counters_setup(l_rgw_cache_list_buckets_first, l_rgw_cache_list_buckets_last, lpcb_init_list_buckets);

  std::unordered_map<int, CountersSetup> setups;
  setups[rgw_put_counters] = put_counters_setup;
  setups[rgw_get_counters] = get_counters_setup;
  setups[rgw_del_obj_counters] = del_obj_counters_setup;
  setups[rgw_del_bucket_counters] = del_bucket_counters_setup;
  setups[rgw_copy_obj_counters] = copy_obj_counters_setup;
  setups[rgw_list_obj_counters] = list_obj_counters_setup;
  setups[rgw_list_buckets_counters] = list_buckets_counters_setup;

  uint64_t target_size = cct->_conf.get_val<uint64_t>("rgw_perf_counters_cache_size");
  bool eviction = cct->_conf.get_val<bool>("rgw_perf_counters_cache_eviction");
  perf_counters_cache = new PerfCountersCache(cct, eviction, target_size, setups); 
  return 0;
}

void add_rgw_put_counters(PerfCountersBuilder *lpcb) {
  // description must match general rgw counters description above
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_CRITICAL);
  lpcb->add_u64_counter(l_rgw_cache_put_ops, "ops", "Puts");
  lpcb->add_u64_counter(l_rgw_cache_put_b, "bytes", "Size of puts");
  lpcb->add_time_avg(l_rgw_cache_put_lat, "lat", "Put latency");
}

void add_rgw_get_counters(PerfCountersBuilder *lpcb) {
  // description must match general rgw counters description above
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_CRITICAL);
  lpcb->add_u64_counter(l_rgw_cache_get_ops, "ops", "Gets");
  lpcb->add_u64_counter(l_rgw_cache_get_b, "bytes", "Size of gets");
  lpcb->add_time_avg(l_rgw_cache_get_lat, "lat", "Get latency");
}

void add_rgw_del_obj_counters(PerfCountersBuilder* lpcb) {
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_CRITICAL);
  lpcb->add_u64_counter(l_rgw_cache_del_obj_ops, "ops", "Delete objects");
  lpcb->add_u64_counter(l_rgw_cache_del_obj_b, "bytes", "Size of delete objects");
  lpcb->add_time_avg(l_rgw_cache_del_obj_lat, "lat", "Delete object latency");
}

void add_rgw_del_bucket_counters(PerfCountersBuilder* lpcb) {
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_CRITICAL);
  lpcb->add_u64_counter(l_rgw_cache_del_bucket_ops, "ops", "Delete Buckets");
  lpcb->add_time_avg(l_rgw_cache_del_bucket_lat, "lat", "Delete bucket latency");
}

void add_rgw_copy_obj_counters(PerfCountersBuilder* lpcb) {
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_CRITICAL);
  lpcb->add_u64_counter(l_rgw_cache_copy_obj_ops, "ops", "Copy objects");
  lpcb->add_u64_counter(l_rgw_cache_copy_obj_b, "bytes", "Size of copy objects");
  lpcb->add_time_avg(l_rgw_cache_copy_obj_lat, "lat", "Copy object latency");
}

void add_rgw_list_obj_counters(PerfCountersBuilder* lpcb) {
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_CRITICAL);
  lpcb->add_u64_counter(l_rgw_cache_list_obj_ops, "ops", "List objects");
  lpcb->add_time_avg(l_rgw_cache_list_obj_lat, "lat", "List objects latency");

}

void add_rgw_list_buckets_counters(PerfCountersBuilder* lpcb) {
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_CRITICAL);
  lpcb->add_u64_counter(l_rgw_cache_list_buckets_ops, "ops", "List buckets");
  lpcb->add_time_avg(l_rgw_cache_list_buckets_lat, "lat", "List buckets latency");
}

void rgw_perf_stop(CephContext *cct)
{
  ceph_assert(perfcounter);
  cct->get_perfcounters_collection()->remove(perfcounter);
  delete perfcounter;
  perf_counters_cache->clear_cache();
  delete perf_counters_cache;
}
