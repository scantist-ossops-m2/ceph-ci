#pragma once

#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"

typedef std::list<std::string> labels_list;

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


class PerfCountersCache {

private:
  CephContext *cct;
  size_t curr_size = 0; 
  size_t target_size = 0; 
  std::unordered_map<std::string_view, CountersSetup> setups;
  mutable ceph::mutex m_lock;

  std::unordered_map<std::string, labels_list::iterator> list_itrs;

  labels_list labels;

  // move recently updated items in the list to the front
  void update_labels_list(const std::string &label) {
    labels.erase(list_itrs[label]);
    list_itrs[label] = labels.insert(labels.begin(), label);
  }

  // removes least recently updated label from labels list
  // removes oldest label's iterator from list_itrs
  void remove_oldest_counters() {
    std::string removed_label = labels.back();
    labels.pop_back();
    auto counters = cct->get_perfcounters_collection()->get(removed_label);
    if(counters) {
      cct->get_perfcounters_collection()->remove(counters);
      counters = NULL;
    }
    list_itrs.erase(removed_label);
    curr_size--;
  }

  // check to make sure key name is non-empty and has labels
  bool check_key(const std::string &key) {
    std::string_view perf_counter_key = ceph::perf_counters::key_name(key);
    // return false for empty key name
    if (perf_counter_key == "") {
      return false;
    }

    // if there are no labels key name is not valid
    auto labels = ceph::perf_counters::key_labels(key);
    if (labels.begin() == labels.end()) {
      return false;
    }

    // don't accept keys where all labels have an empty label name
    bool valid_label_pair = false;
    for (auto label : labels) {
      if (label.first == "") {
        continue;
      } else {
        // a label in the set of labels is valid so entire key is valid
        // labels with empty strings are not dumped into json
        valid_label_pair = true;
        break;
      }
    }

    if (!valid_label_pair) {
      return false;
    }

    return true;
  }

  ceph::common::PerfCounters* get(const std::string &key) {
    return cct->get_perfcounters_collection()->get(key);
  }

  PerfCounters* add(const std::string &key) {
    // key is not valid, these counters will not be get created
    if (!check_key(key))
      return NULL;

    auto counters = get(key);
    if (!counters) {
      // check to make sure cache isn't full
      if (curr_size >= target_size) {
        remove_oldest_counters();
      }

      // get specific setup from setups
      std::string_view counter_type = ceph::perf_counters::key_name(key);
      CountersSetup pb = setups[counter_type];

      // perf counters instance creation code
      PerfCountersBuilder lpcb(cct, key, pb.first, pb.last);
      pb.add_counters(&lpcb);

      // add counters to builder
      counters = lpcb.create_perf_counters();

      // add new counters to collection, cache
      cct->get_perfcounters_collection()->add(counters);
      labels_list::iterator pos = labels.insert(labels.begin(), key);
      list_itrs[key] = pos;
      curr_size++;
    }
    return counters;
  }



public:

  size_t get_cache_size() {
    std::lock_guard lock(m_lock);
    return curr_size;
  }

  void inc(const std::string &label, int indx, uint64_t v) {
    std::lock_guard lock(m_lock);
    auto counters = get(label);
    if (counters) {
      counters->inc(indx, v);
    } else {
      auto new_counters = add(label);
      if (new_counters) {
        new_counters->inc(indx, v);
      }
    }
  }

  void dec(const std::string &label, int indx, uint64_t v) {
    std::lock_guard lock(m_lock);
    auto counters = get(label);
    if (counters) {
      counters->dec(indx, v);
    } else {
      auto new_counters = add(label);
      if (new_counters) {
        new_counters->dec(indx, v);
      }
    }
  }

  void tinc(const std::string &label, int indx, utime_t amt) {
    std::lock_guard lock(m_lock);
    auto counters = get(label);
    if (counters) {
      counters->tinc(indx, amt);
    } else {
      auto new_counters = add(label);
      if (new_counters) {
        new_counters->tinc(indx, amt);
      }
    }
  }

  void tinc(const std::string &label, int indx, ceph::timespan amt) {
    std::lock_guard lock(m_lock);
    auto counters = get(label);
    if (counters) {
      counters->tinc(indx, amt);
    } else {
      auto new_counters = add(label);
      if (new_counters) {
        new_counters->tinc(indx, amt);
      }
    }
  }

  void set_counter(const std::string &label, int indx, uint64_t val) {
    std::lock_guard lock(m_lock);
    auto counters = get(label);
    if (counters) {
      counters->set(indx, val);
    } else {
      auto new_counters = add(label);
      if (new_counters) {
        new_counters->set(indx, val);
      }
    }
  }

  uint64_t get_counter(const std::string &label, int indx) {
    std::lock_guard lock(m_lock);
    auto counters = get(label);
    uint64_t val = 0;
    if (counters) {
      val = counters->get(indx);
    }
    return val;
  }

  utime_t tget(const std::string &label, int indx) {
    std::lock_guard lock(m_lock);
    auto counters = get(label);
    utime_t val;
    if (counters) {
      val = counters->tget(indx);
      return val;
    } else {
      return utime_t();
    }
  }

  void tset(const std::string &label, int indx, utime_t amt) {
    std::lock_guard lock(m_lock);
    auto counters = get(label);
    if (counters) {
      counters->tset(indx, amt);
    } else {
      auto new_counters = add(label);
      if (new_counters) {
        new_counters->tset(indx, amt);
      }
    }
  }

  // for use right before destructor would get called
  void clear_cache() {
    std::lock_guard lock(m_lock);
    for (auto it = labels.begin(); it != labels.end(); ++it ) {
      auto counters = get(*it);
      if(counters) {
        cct->get_perfcounters_collection()->remove(counters);
        counters = NULL;
      }
      list_itrs.erase(*it);
      curr_size--;
    }
  }

  PerfCountersCache(CephContext *_cct, size_t _target_size, std::unordered_map<std::string_view, CountersSetup> _setups) : cct(_cct), 
      target_size(_target_size), setups(_setups), m_lock(ceph::make_mutex("PerfCountersCache")) {}

  ~PerfCountersCache() {}

};
