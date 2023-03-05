// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef GLOBALSNAPMAPPER_H
#define GLOBALSNAPMAPPER_H

#include <string>
#include <set>
#include <utility>
#include <cstring>

#include "common/map_cacher.hpp"
#include "common/hobject.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/object.h"
#include "os/ObjectStore.h"
#include "osd/OSDMap.h"

template<> struct std::hash<snapid_t> {
  size_t operator()(const snapid_t &r) const {
    static rjhash<uint64_t> I;
    return I(r.val);
  }
};


static constexpr uint16_t NO_SM_SHARD_ID      = 128;
static constexpr uint16_t MAX_SM_SHARD_ID     = NO_SM_SHARD_ID + 1;
static constexpr uint16_t ILLEGAL_SM_SHARD_ID = MAX_SM_SHARD_ID + 1;

using shard_tables_t = std::array<snap_to_objs_map_t*, MAX_SM_SHARD_ID>;

class GlobalSnapMapper;

class SnapMapperShard {
  friend class GlobalSnapMapper;
  friend std::ostream& operator<<(std::ostream& lhs, const SnapMapperShard& rhs)
  {
    return lhs << (unsigned)rhs.id;
  }

public:
  SnapMapperShard(shard_id_t shard=shard_id_t::NO_SHARD):id(shard==shard_id_t::NO_SHARD? NO_SM_SHARD_ID: shard.id)
  { }
  uint16_t get_id() const { return id; }
  DENC(SnapMapperShard, v, p) {
    denc(v.id, p);
  }

  bool operator==(const SnapMapperShard&) const = default;
  auto operator<=>(const SnapMapperShard&) const = default;
private:
  void set_id(unsigned id) {
    id = id;
  }
  uint16_t id;
};
WRITE_CLASS_DENC(SnapMapperShard)


/**
 * GlobalSnapMapper
 *
 * An array indexed by SnapMapperShard holding maps from snapid -> {hobject_t}
 *
 * All objects in a particular snap are stored in the same map.
 * Mapping is arranged such that all objects in a pg for a
 * particular snap will group a single hash prefix.
 */
class GlobalSnapMapper {
public:
  static const std::string MAPPING_PREFIX;
  static const std::string OBJECT_PREFIX;

  struct object_snaps {
    hobject_t oid;
    std::set<snapid_t> snaps;
    object_snaps(hobject_t oid, const std::set<snapid_t> &snaps)
      : oid(oid), snaps(snaps) {}
    object_snaps() {}
    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &bp);
  };

  GlobalSnapMapper(CephContext* cct) : cct(cct) {
    for (unsigned i = 0; i < MAX_SM_SHARD_ID; i++) {
      snap_to_objs_arr[i] = nullptr;
    }
  }

  ~GlobalSnapMapper() {
    for (unsigned i = 0; i < MAX_SM_SHARD_ID; i++) {
      if (snap_to_objs_arr[i]) {
	delete snap_to_objs_arr[i];
      }
    }
  }

  uint64_t count_objects() const;
  void     print_snaps(const char *s) const;

  /// Add mapping for oid, must not already be mapped
  void add_oid(SnapMapperShard shard, const hobject_t &oid_to_add, const std::vector<snapid_t>& new_snaps);
  void add_oid(SnapMapperShard shard, const hobject_t &oid_to_add, snapid_t snapid);

  /// Update snaps for oid, empty new_snaps removes the mapping
  int update_snaps(
    SnapMapperShard              shard,
    const hobject_t             &oid,       ///< [in] oid to update
    const std::vector<snapid_t> &new_snaps, ///< [in] new snap vector
    const std::vector<snapid_t> &old_snaps  ///< [in] old snap vector
		   ); ///@ return error, 0 on success

  uint64_t delete_objs_from_pg(SnapMapperShard    shard,
			       int64_t            pool,
			       const spg_t       &pgid,
			       uint32_t           hash_prefix,
			       uint32_t           hash_prefix_reversed,
			       uint32_t           mask_bits,
			       uint32_t           match,
			       uint64_t           max_count);

  uint64_t count_objects_per_pg(SnapMapperShard     shard,
				int64_t             pool,
				const spg_t        &pgid,
				uint32_t            hash_prefix,
				uint32_t            hash_prefix_reversed,
				uint32_t            mask_bits,
				uint32_t            match);

  /// Returns first object with snap as a snap
  ///< @return error, -ENOENT if no more objects
  int get_next_objects_to_trim(SnapMapperShard         shard,
			       int64_t                 pool,
			       const spg_t            &pgid,
			       uint32_t                hash_prefix,
			       uint32_t                hash_prefix_reversed,
			       uint32_t                mask_bits,
			       uint32_t                match,
			       snapid_t                snap,
			       unsigned                max_count,
			       std::vector<hobject_t> *out);

  /// Remove mapping for oid
  ///< @return error, -ENOENT if the object is not mapped
  int remove_oid_from_all_snaps(SnapMapperShard              shard,
				const hobject_t             &oid_to_remove,
				const std::vector<snapid_t> &old_snaps )
  {
    return _remove_oid(shard, oid_to_remove, old_snaps);
  }

  // clear all data stored by this mapper (done before collection removal)
  //int reset();

  shard_tables_t::const_iterator cbegin() const noexcept {
    return snap_to_objs_arr.begin();
  }

  shard_tables_t::const_iterator cend() const noexcept {
    return snap_to_objs_arr.end();
  }

  SnapMapperShard get_snap_mapper_shard(const snap_to_objs_map_t* snap_to_objs) const;
  int             get_snaps_for_scrubber(SnapMapperShard     shard,
					 const hobject_t    &coid,
					 std::set<snapid_t> &out);
  int             get_objs_for_scrubber(SnapMapperShard         shard,
					snapid_t                snap,
					std::vector<hobject_t> &out,
					unsigned                count) const;
  CephContext* cct;
private:
  snap_to_objs_map_t* create_snap_to_objs(SnapMapperShard shard)
  {
    ceph_assert(snap_to_objs_arr[shard.get_id()] == nullptr);
    snap_to_objs_arr[shard.get_id()] = new snap_to_objs_map_t;
    return snap_to_objs_arr[shard.get_id()];
  }

  snap_to_objs_map_t* get_snap_to_objs(SnapMapperShard shard)
  {
    return snap_to_objs_arr[shard.get_id()];
  }

  const snap_to_objs_map_t* get_snap_to_objs(SnapMapperShard shard) const
  {
    return snap_to_objs_arr[shard.get_id()];
  }

  int  report_bogus_shard(const char *func, SnapMapperShard shard) const;
  bool check(const hobject_t &hoid, uint32_t mask_bits, uint32_t match) const;
  int  remove_mapping_from_snapid_to_hobject(SnapMapperShard shard, const hobject_t& oid, const snapid_t & snapid);
  int  _remove_oid(SnapMapperShard shard, const hobject_t &oid, const std::vector<snapid_t> &old_snaps);

  uint64_t delete_objs_from_pg_snap(snap_to_objs_map_t *snap_to_objs,
				    SnapMapperShard     shard,
				    int64_t             pool,
				    const spg_t        &pgid,
				    uint32_t            hash_prefix,
				    uint32_t            hash_prefix_reversed,
				    uint32_t            mask_bits,
				    uint32_t            match,
				    snapid_t            snapid,
				    uint64_t            max_count);

  uint64_t count_objects_per_pg_snap(snap_to_objs_map_t *snap_to_objs,
				     SnapMapperShard     shard,
				     int64_t             pool,
				     const spg_t        &pgid,
				     uint32_t            hash_prefix,
				     uint32_t            hash_prefix_reversed,
				     uint32_t            mask_bits,
				     uint32_t            match,
				     snapid_t            snapid);

  shard_tables_t snap_to_objs_arr{};

  mutable std::shared_mutex  m_mutex;
  bool                       is_disabled  = false;
};
WRITE_CLASS_ENCODER(GlobalSnapMapper::object_snaps)
#endif
