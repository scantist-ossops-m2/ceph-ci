#pragma once

#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/intrusive_lru.h"

template <typename LRUItem>
struct item_to_key {
  using type = std::string;
  const type &operator()(const LRUItem &item) {
    return item.key;
  }
};

struct PerfCountersCacheEntry : public ceph::common::intrusive_lru_base<
                                       ceph::common::intrusive_lru_config<
                                       std::string, PerfCountersCacheEntry, item_to_key<PerfCountersCacheEntry>>> {
  std::string key;
  std::shared_ptr<PerfCounters> counters;
  CephContext *cct;

  PerfCountersCacheEntry(const std::string &_key) : key(_key) {}

  ~PerfCountersCacheEntry() {
    if(counters) {
      cct->get_perfcounters_collection()->remove(counters.get());
    }
  }
};

class CountersSetup {

public:
  int first;
  int last;
  std::function<void(PerfCountersBuilder*)> add_counters;

  CountersSetup() : first(0), last(0) {}

  CountersSetup(int _first, int _last, std::function<void(PerfCountersBuilder*)> _add_counters) {
    first = _first;
    last = _last;
    add_counters = _add_counters;
  }

  CountersSetup(const CountersSetup& cs) {
    first = cs.first;
    last = cs.last;
    add_counters = cs.add_counters;
  }
};


class PerfCountersCache : public PerfCountersCacheEntry::lru_t {
private:
  CephContext *cct;
  std::unordered_map<std::string_view, CountersSetup> setups;
  mutable ceph::mutex m_lock;

  /* check to make sure key name is non-empty and non-empty labels
   *
   * A valid key has the the form 
   * key:label1:val1:label2:val2 ... labelN:valN
   * The following 3 properties checked for in this function
   * 1. A non-empty key
   * 2. At least 1 set of labels
   * 3. Each label has a non-empty key and value
   *
   */
  void check_key(const std::string &key) {
    std::string_view key_name = ceph::perf_counters::key_name(key);
    // return false for empty key name
    ceph_assert(key_name != "");

    // if there are no labels key name is not valid
    auto key_labels = ceph::perf_counters::key_labels(key);
    ceph_assert(key_labels.begin() != key_labels.end());

    // don't accept keys where any labels have an empty label name
    for (auto key_label : key_labels) {
      ceph_assert(key_label.first != "");
      ceph_assert(key_label.second != "");
    }
  }

  std::shared_ptr<PerfCounters> add(const std::string &key) {
    check_key(key);

    auto [ref, key_existed] = get_or_create(key);
    if (!key_existed) {
      // get specific setup from setups
      std::string_view counter_type = ceph::perf_counters::key_name(key);
      CountersSetup pb = setups[counter_type];

      // perf counters instance creation code
      PerfCountersBuilder lpcb(cct, key, pb.first, pb.last);
      pb.add_counters(&lpcb);

      // add counters to builder
      std::shared_ptr<PerfCounters> new_counters(lpcb.create_perf_counters());

      // add new counters to collection, cache
      cct->get_perfcounters_collection()->add(new_counters.get());
      ref->counters = new_counters;
      ref->cct = cct;
    }
    return ref->counters;
  }

public:

  // get() and its associated shared_ptr reference counting should be avoided 
  // unless the caller intends to modify multiple counter values at the same time.
  // If multiple counter values will not be modified at the same time, inc/dec/etc. 
  // are recommended.
  std::shared_ptr<PerfCounters> get(const std::string &key) {
    std::lock_guard lock(m_lock);
    return add(key);
  }

  void inc(const std::string &key, int indx, uint64_t v) {
    std::lock_guard lock(m_lock);
    auto counters = add(key);
    if (counters) {
      counters->inc(indx, v);
    }
  }

  void dec(const std::string &key, int indx, uint64_t v) {
    std::lock_guard lock(m_lock);
    auto counters = add(key);
    if (counters) {
      counters->dec(indx, v);
    }
  }

  void tinc(const std::string &key, int indx, utime_t amt) {
    std::lock_guard lock(m_lock);
    auto counters = add(key);
    if (counters) {
      counters->tinc(indx, amt);
    }
  }

  void tinc(const std::string &key, int indx, ceph::timespan amt) {
    std::lock_guard lock(m_lock);
    auto counters = add(key);
    if (counters) {
      counters->tinc(indx, amt);
    }
  }

  void set_counter(const std::string &key, int indx, uint64_t val) {
    std::lock_guard lock(m_lock);
    auto counters = add(key);
    if (counters) {
      counters->set(indx, val);
    }
  }

  uint64_t get_counter(const std::string &key, int indx) {
    std::lock_guard lock(m_lock);
    auto counters = add(key);
    uint64_t val = 0;
    if (counters) {
      val = counters->get(indx);
    }
    return val;
  }

  utime_t tget(const std::string &key, int indx) {
    std::lock_guard lock(m_lock);
    auto counters = add(key);
    utime_t val;
    if (counters) {
      val = counters->tget(indx);
      return val;
    } else {
      return utime_t();
    }
  }

  void tset(const std::string &key, int indx, utime_t amt) {
    std::lock_guard lock(m_lock);
    auto counters = add(key);
    if (counters) {
      counters->tset(indx, amt);
    }
  }

  PerfCountersCache(CephContext *_cct, size_t _target_size, std::unordered_map<std::string_view, CountersSetup> _setups) : cct(_cct), 
      setups(_setups), m_lock(ceph::make_mutex("PerfCountersCache")) { set_target_size(_target_size); }

  ~PerfCountersCache() { set_target_size(0); }

};
