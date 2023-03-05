// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "ScrubStore.h"
#include "osd/osd_types.h"
#include "common/scrub_types.h"
#include "include/rados/rados_types.hpp"

using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

#ifdef WITH_SEASTAR
#include "crimson/common/log.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
  template <typename ValuesT = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, ValuesT>;
  using interruptor =
    ::crimson::interruptible::interruptor<
      ::crimson::osd::IOInterruptCondition>;

#define CRIMSON_DEBUG(FMT_MSG, ...) crimson::get_logger(ceph_subsys_).debug(FMT_MSG, ##__VA_ARGS__)
int OSDriver::get_keys(
  const std::set<std::string> &keys,
  std::map<std::string, ceph::buffer::list> *out)
{
  CRIMSON_DEBUG("OSDriver::{}:{}", __func__, __LINE__);
  using crimson::os::FuturizedStore;
  return interruptor::green_get(os->omap_get_values(
    ch, hoid, keys
  ).safe_then([out] (FuturizedStore::omap_values_t&& vals) {
    // just the difference in comparator (`std::less<>` in omap_values_t`)
    reinterpret_cast<FuturizedStore::omap_values_t&>(*out) = std::move(vals);
    return 0;
  }, FuturizedStore::read_errorator::all_same_way([] (auto& e) {
    assert(e.value() > 0);
    return -e.value();
  }))); // this requires seastar::thread
  CRIMSON_DEBUG("OSDriver::{}:{}", __func__, __LINE__);
}

int OSDriver::get_next(
  const std::string &key,
  std::pair<std::string, ceph::buffer::list> *next)
{
  CRIMSON_DEBUG("OSDriver::{}:{}", __func__, __LINE__);
  using crimson::os::FuturizedStore;
  return interruptor::green_get(os->omap_get_values(
    ch, hoid, key
  ).safe_then_unpack([&key, next] (bool, FuturizedStore::omap_values_t&& vals) {
    CRIMSON_DEBUG("OSDriver::{}:{}", "get_next", __LINE__);
    if (auto nit = std::begin(vals); nit == std::end(vals)) {
      CRIMSON_DEBUG("OSDriver::{}:{}", "get_next", __LINE__);
      return -ENOENT;
    } else {
      CRIMSON_DEBUG("OSDriver::{}:{}", "get_next", __LINE__);
      assert(nit->first > key);
      *next = *nit;
      return 0;
    }
  }, FuturizedStore::read_errorator::all_same_way([] {
    CRIMSON_DEBUG("OSDriver::{}:{}", "get_next", __LINE__);
    return -EINVAL;
  }))); // this requires seastar::thread
  CRIMSON_DEBUG("OSDriver::{}:{}", __func__, __LINE__);
}

int OSDriver::get_next_or_current(
  const std::string &key,
  std::pair<std::string, ceph::buffer::list> *next_or_current)
{
  CRIMSON_DEBUG("OSDriver::{}:{}", __func__, __LINE__);
  using crimson::os::FuturizedStore;
  // let's try to get current first
  return interruptor::green_get(os->omap_get_values(
    ch, hoid, FuturizedStore::omap_keys_t{key}
  ).safe_then([&key, next_or_current] (FuturizedStore::omap_values_t&& vals) {
    assert(vals.size() == 1);
    *next_or_current = std::make_pair(key, std::move(vals[0]));
    return 0;
  }, FuturizedStore::read_errorator::all_same_way(
    [next_or_current, &key, this] {
    // no current, try next
    return get_next(key, next_or_current);
  }))); // this requires seastar::thread
  CRIMSON_DEBUG("OSDriver::{}:{}", __func__, __LINE__);
}
#else
int OSDriver::get_keys(
  const std::set<std::string> &keys,
  std::map<std::string, ceph::buffer::list> *out)
{
  return os->omap_get_values(ch, hoid, keys, out);
}

int OSDriver::get_next(
  const std::string &key,
  std::pair<std::string, ceph::buffer::list> *next)
{
  ObjectMap::ObjectMapIterator iter =
    os->get_omap_iterator(ch, hoid);
  if (!iter) {
    ceph_abort();
    return -EINVAL;
  }
  iter->upper_bound(key);
  if (iter->valid()) {
    if (next)
      *next = make_pair(iter->key(), iter->value());
    return 0;
  } else {
    return -ENOENT;
  }
}

int OSDriver::get_next_or_current(
  const std::string &key,
  std::pair<std::string, ceph::buffer::list> *next_or_current)
{
  ObjectMap::ObjectMapIterator iter =
    os->get_omap_iterator(ch, hoid);
  if (!iter) {
    ceph_abort();
    return -EINVAL;
  }
  iter->lower_bound(key);
  if (iter->valid()) {
    if (next_or_current)
      *next_or_current = make_pair(iter->key(), iter->value());
    return 0;
  } else {
    return -ENOENT;
  }
}
#endif // WITH_SEASTAR


namespace {
ghobject_t make_scrub_object(const spg_t& pgid)
{
  ostringstream ss;
  ss << "scrub_" << pgid;
  return pgid.make_temp_ghobject(ss.str());
}

string first_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0x00000000,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

// the object_key should be unique across pools
string to_object_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(object_t(oid.name),
			oid.locator, // key
			oid.snap,
			0,		// hash
			pool,
			oid.nspace);
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string last_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0xffffffff,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // the representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0x00000000,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string to_snap_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(object_t(oid.name),
			oid.locator, // key
			oid.snap,
			0x77777777, // hash
			pool,
			oid.nspace);
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string last_snap_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0xffffffff,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}
}

namespace Scrub {

Store*
Store::create(ObjectStore* store,
	      ObjectStore::Transaction* t,
	      const spg_t& pgid,
	      const coll_t& coll)
{
  ceph_assert(store);
  ceph_assert(t);
  ghobject_t oid = make_scrub_object(pgid);
  t->touch(coll, oid);
  return new Store{coll, oid, store};
}

Store::Store(const coll_t& coll, const ghobject_t& oid, ObjectStore* store)
  : coll(coll),
    hoid(oid),
    driver(store, coll, hoid),
    backend(&driver)
{}

Store::~Store()
{
  ceph_assert(results.empty());
}

void Store::add_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  add_object_error(pool, e);
}

void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  results[to_object_key(pool, e.object)] = bl;
}

void Store::add_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  add_snap_error(pool, e);
}

void Store::add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  results[to_snap_key(pool, e.object)] = bl;
}

bool Store::empty() const
{
  return results.empty();
}

void Store::flush(ObjectStore::Transaction* t)
{
  if (t) {
    OSDriver::OSTransaction txn = driver.get_transaction(t);
    backend.set_keys(results, &txn);
  }
  results.clear();
}

void Store::cleanup(ObjectStore::Transaction* t)
{
  t->remove(coll, hoid);
}

std::vector<bufferlist>
Store::get_snap_errors(int64_t pool,
		       const librados::object_id_t& start,
		       uint64_t max_return) const
{
  const string begin = (start.name.empty() ?
			first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);
  return get_errors(begin, end, max_return);
}

std::vector<bufferlist>
Store::get_object_errors(int64_t pool,
			 const librados::object_id_t& start,
			 uint64_t max_return) const
{
  const string begin = (start.name.empty() ?
			first_object_key(pool) : to_object_key(pool, start));
  const string end = last_object_key(pool);
  return get_errors(begin, end, max_return);
}

std::vector<bufferlist>
Store::get_errors(const string& begin,
		  const string& end,
		  uint64_t max_return) const
{
  vector<bufferlist> errors;
  auto next = std::make_pair(begin, bufferlist{});
  while (max_return && !backend.get_next(next.first, &next)) {
    if (next.first >= end)
      break;
    errors.push_back(next.second);
    max_return--;
  }
  return errors;
}

} // namespace Scrub
