#pragma once

#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "common/ceph_context.h"

typedef std::list<std::string> labels_list;

struct CacheEntry {
  PerfCounters *counters;
  labels_list::iterator pos;

  CacheEntry(PerfCounters* _counters, labels_list::iterator _pos) {
    counters = _counters;
    pos = _pos;
  }

  ~CacheEntry() {}
};

class PerfCountersCache {

private:
  CephContext *cct;
  bool eviction;
  size_t curr_size = 0; 
  size_t target_size = 0; 
  int lower_bound = 0;
  int upper_bound = 0;
  std::function<void(PerfCountersBuilder*)> lpcb_init;

  std::unordered_map<std::string, CacheEntry*> cache;

  labels_list labels;

  // move recently updated items in the list to the front
  void update_labels_list(std::string label) {
    labels.erase(cache[label]->pos);
    cache[label]->pos = labels.insert(labels.begin(), label);
  }

  // removes least recently updated label from labels list
  // removes oldest label's CacheEntry from cache
  void remove_oldest_counters() {
    std::string removed_label = labels.back();
    labels.pop_back();

    ceph_assert(cache[removed_label]->counters);
    cct->get_perfcounters_collection()->remove(cache[removed_label]->counters);
    delete cache[removed_label]->counters;
    cache[removed_label]->counters = NULL;

    delete cache[removed_label];
    cache[removed_label] = NULL;

    cache.erase(removed_label);
    curr_size--;
  }

  // check to make sure key name is valid and has labels
  bool check_key(std::string key) {
    std::string_view perf_counter_key = ceph::perf_counters::key_name(key);
    // return false for empty key name
    if(perf_counter_key == "") {
      return false;
    }

    // if there are no labels key name is not valid
    auto labels = ceph::perf_counters::key_labels(key);
    if(labels.begin() == labels.end()) {
      return false;
    }

    bool valid_label_pair = false;
    for (auto label : labels) {
      // don't dump labels with empty label names or values
      if(label.first == "" || label.second == "") {
        continue;
      } else {
        // a label in the set of labels is valid so entire key is valid
        // labels with empty strings are not dumped into json
        valid_label_pair = true;
        break;
      }
    }
    if(!valid_label_pair) {
      return false;
    }

    return true;
  }

public:

  size_t get_cache_size() {
    return curr_size;
  }

  PerfCounters* get(std::string key) {
    auto got = cache.find(key);
    if(got != cache.end()) {
      return got->second->counters;
    }
    return NULL;
  }

  ceph::common::PerfCounters* add(std::string key, bool is_labeled = true) {
    // key is not valid, these counters will not be get created
    if(!check_key(key))
      return NULL;

    auto counters = get(key);
    if (!counters) {
      // check to make sure cache isn't full
      if(eviction) {
        if(curr_size >= target_size) {
          remove_oldest_counters();
        }
      }

      // perf counters instance creation code
      auto lpcb = new PerfCountersBuilder(cct, key, lower_bound, upper_bound, is_labeled);
      lpcb_init(lpcb);

      // add counters to builder
      counters = lpcb->create_perf_counters();
      delete lpcb;

      // add new counters to collection, cache
      cct->get_perfcounters_collection()->add(counters);
      labels_list::iterator pos = labels.insert(labels.begin(), key);
      CacheEntry *m = new CacheEntry(counters, pos);
      cache[key] = m;
      curr_size++;
    }
    return counters;
  }

  void inc(std::string label, int indx, uint64_t v) {
    auto counters = get(label);
    if(counters) {
      counters->inc(indx, v);
    }
  }

  void dec(std::string label, int indx, uint64_t v) {
    auto counters = get(label);
    if(counters) {
      counters->dec(indx, v);
    }
  }

  void tinc(std::string label, int indx, utime_t amt) {
    auto counters = get(label);
    if(counters) {
      counters->tinc(indx, amt);
    }
  }

  void tinc(std::string label, int indx, ceph::timespan amt) {
    auto counters = get(label);
    if(counters) {
      counters->tinc(indx, amt);
    }
  }

  void set_counter(std::string label, int indx, uint64_t val) {
    auto counters = get(label);
    if(counters) {
      counters->set(indx, val);
    }
  }

  uint64_t get_counter(std::string label, int indx) {
    auto counters = get(label);
    uint64_t val = 0;
    if(counters) {
      val = counters->get(indx);
    }
    return val;
  }

  utime_t tget(std::string label, int indx) {
    auto counters = get(label);
    utime_t val;
    if(counters) {
      val = counters->tget(indx);
      return val;
    } else {
      return utime_t();
    }
  }

  void tset(std::string label, int indx, utime_t amt) {
    auto counters = get(label);
    if(counters) {
      counters->tset(indx, amt);
    }
  }

  // for use right before destructor would get called
  void clear_cache() {
    for(auto it = cache.begin(); it != cache.end(); ++it ) {
      ceph_assert(it->second->counters);
      cct->get_perfcounters_collection()->remove(it->second->counters);
      delete it->second->counters;
      // delete CacheEntry*
      delete it->second;
      curr_size--;
    }
  }

  PerfCountersCache(CephContext *_cct, bool _eviction, size_t _target_size, int _lower_bound, int _upper_bound, 
      std::function<void(PerfCountersBuilder*)> _lpcb_init) : cct(_cct), 
      eviction(_eviction), target_size(_target_size), lower_bound(_lower_bound), upper_bound(_upper_bound), 
      lpcb_init(_lpcb_init) {}

  ~PerfCountersCache() {}

};
