// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_SCRUB_RESULT_H
#define CEPH_SCRUB_RESULT_H

#include "common/hobject.h"
#include "common/map_cacher.hpp"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/object.h"
#include "os/ObjectStore.h"
#include "osd/OSDMap.h"
#include "osd/SnapMapReaderI.h"

class OSDriver : public MapCacher::StoreDriver<std::string, ceph::buffer::list> {
#ifdef WITH_SEASTAR
  using ObjectStoreT = crimson::os::FuturizedStore::Shard;
  using CollectionHandleT = ObjectStoreT::CollectionRef;
#else
  using ObjectStoreT = ObjectStore;
  using CollectionHandleT = ObjectStoreT::CollectionHandle;

#endif
  ObjectStoreT *os;
  CollectionHandleT ch;
  ghobject_t hoid;

public:
  class OSTransaction : public MapCacher::Transaction<std::string, ceph::buffer::list> {
    friend class OSDriver;
    coll_t cid;
    ghobject_t hoid;
    ceph::os::Transaction *t;
    OSTransaction(
      const coll_t &cid,
      const ghobject_t &hoid,
      ceph::os::Transaction *t)
      : cid(cid), hoid(hoid), t(t) {}
  public:
    void set_keys(
      const std::map<std::string, ceph::buffer::list> &to_set) override {
      t->omap_setkeys(cid, hoid, to_set);
    }
    void remove_keys(
      const std::set<std::string> &to_remove) override {
      t->omap_rmkeys(cid, hoid, to_remove);
    }
    void add_callback(
      Context *c) override {
      t->register_on_applied(c);
    }
  };

  OSTransaction get_transaction(
    ceph::os::Transaction *t) const {
    return OSTransaction(ch->get_cid(), hoid, t);
  }

#ifndef WITH_SEASTAR
  OSDriver(ObjectStoreT *os, const coll_t& cid, const ghobject_t &hoid) :
    OSDriver(os, os->open_collection(cid), hoid) {}
#endif
  OSDriver(ObjectStoreT *os, CollectionHandleT ch, const ghobject_t &hoid) :
    os(os),
    ch(ch),
    hoid(hoid) {}

  int get_keys(
    const std::set<std::string> &keys,
    std::map<std::string, ceph::buffer::list> *out) override;
  int get_next(
    const std::string &key,
    std::pair<std::string, ceph::buffer::list> *next) override;
  int get_next_or_current(
    const std::string &key,
    std::pair<std::string, ceph::buffer::list> *next_or_current) override;
};

namespace librados {
struct object_id_t;
}

struct inconsistent_obj_wrapper;
struct inconsistent_snapset_wrapper;

namespace Scrub {

class Store {
 public:
  ~Store();
  static Store* create(ObjectStore* store,
		       ObjectStore::Transaction* t,
		       const spg_t& pgid,
		       const coll_t& coll);
  void add_object_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e);

  // and a variant-friendly interface:
  void add_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_error(int64_t pool, const inconsistent_snapset_wrapper& e);

  bool empty() const;
  void flush(ObjectStore::Transaction*);
  void cleanup(ObjectStore::Transaction*);

  std::vector<ceph::buffer::list> get_snap_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const;

  std::vector<ceph::buffer::list> get_object_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const;

  OSDriver::OSTransaction get_transaction(ceph::os::Transaction *t) {
    return driver.get_transaction(t);
  }
 private:
  Store(const coll_t& coll, const ghobject_t& oid, ObjectStore* store);
  std::vector<ceph::buffer::list> get_errors(const std::string& start,
					     const std::string& end,
					     uint64_t max_return) const;
 private:
  const coll_t coll;
  const ghobject_t hoid;
  // a temp object holding mappings from seq-id to inconsistencies found in
  // scrubbing

  OSDriver driver;
  mutable MapCacher::MapCacher<std::string, ceph::buffer::list> backend;
  std::map<std::string, ceph::buffer::list> results;
};
}  // namespace Scrub

#endif	// CEPH_SCRUB_RESULT_H
