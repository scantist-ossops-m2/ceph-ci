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

#ifndef PGSNAPMAPPER_H
#define PGSNAPMAPPER_H

#include <string>
#include <set>
#include <utility>
#include <cstring>

#include "common/hobject.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/object.h"
#include "os/ObjectStore.h"
#include "osd/OSDMap.h"
#include "osd/SnapMapReaderI.h"
#include "osd/GlobalSnapMapper.h"

/**
 * PGSnapMapper
 *
 *  This Class is a per-PG interface to the global Snap Mapper.
 *  It doesn't hold a DB of itself, but instead uses a ref to DB held by
 *   the global SnapMapper.
 *  This API maintains the PG perfixes built from the shard/pool/hash and
 *   uses them when asked to retrive a set hobjects mapped to a snapid and
 *   owned by this PG.
 *  snapid -> {hobject_t}
 *
 *  mapping is arranged such that all objects in a particular
 *  snap will sort together under a single prefix
 */
class PGSnapMapper : public Scrub::SnapMapReaderI {
public:
  PGSnapMapper(GlobalSnapMapper *gsnap_ref,
	       CephContext*      cct,
	       spg_t             pgid,
	       uint32_t          match,
	       uint32_t          current_split_bits,
	       int64_t           pool,
	       shard_id_t        shard)
    : cct(cct), gsnap_ref(gsnap_ref), pgid(pgid), mask_bits(current_split_bits), match(match), pool(pool), shard(shard)
  {
    ceph_assert(gsnap_ref);
    update_bits(mask_bits);
    is_disabled = false;
  }

  /// Update bits in case of pg split or merge
  void update_bits(uint32_t new_split_bits);

  // count objcets owned by this PG
  uint32_t count_objects();
  // print objcets owned by this PG
  void     print_snaps(const char *s) const { gsnap_ref->print_snaps(s);}

  /// Update snaps for oid, empty new_snaps removes the mapping
  int update_snaps(
    const hobject_t             &coid,      ///< [in] oid to update
    const std::vector<snapid_t> &new_snaps, ///< [in] new snap vector
    const std::vector<snapid_t> &old_snaps  ///< [in] old snap vector
		   )
  {
    ceph_assert(check(coid));
    return gsnap_ref->update_snaps(shard, coid, new_snaps, old_snaps);
  }

  /// Add mapping for oid, must not already be mapped
  void add_oid(const hobject_t &oid_to_add, const std::vector<snapid_t>& new_snaps)
  {
    ceph_assert(check(oid_to_add));
    return gsnap_ref->add_oid(shard, oid_to_add, new_snaps);
  }

  /// Remove mapping for oid
  ///< @return error, -ENOENT if the object is not mapped
  int remove_oid_from_all_snaps(const hobject_t             &oid_to_remove,
				const std::vector<snapid_t> &old_snaps )
  {
    ceph_assert(check(oid_to_remove));
    return gsnap_ref->remove_oid_from_all_snaps(shard, oid_to_remove, old_snaps);
  }

  /// Returns first object with snap as a snap
  ///< @return error, -ENOENT if no more objects
  int get_next_objects_to_trim(snapid_t snap, unsigned max_count, std::vector<hobject_t> *out) {
    return gsnap_ref->get_next_objects_to_trim(shard, pool, pgid, hash_prefix, hash_prefix_reversed, mask_bits, match, snap, max_count, out);
  }

  // clear all data stored by this mapper (done before collection removal)
  int reset() {
    //return gsnap_ref->delete_pg(shard, pool, pgid, hash_prefix, hash_prefix_reversed, mask_bits, match);
    return delete_objs(UINT64_MAX);
  }

  // clear @count objs from this PG mapper (done before collection removal)
  int delete_objs(uint64_t count) {
    return gsnap_ref->delete_objs_from_pg(shard, pool, pgid, hash_prefix, hash_prefix_reversed, mask_bits, match, count);
  }

  /// Get snaps for oid - alternative interface
  tl::expected<std::set<snapid_t>, SnapMapReaderI::result_t> get_snaps(const hobject_t &hoid) const final;

  /**
   * get_snaps_check_consistency
   *
   * Returns snaps for hoid as in get_snaps(), but additionally validates the
   * snap->hobject_t mappings ('SNA_' entries).
   */
  tl::expected<std::set<snapid_t>, SnapMapReaderI::result_t>
  get_snaps_check_consistency(const hobject_t &hoid) const final
  {
    return get_snaps(hoid);
  }

  CephContext* cct;

private:
  bool report_check_error(const hobject_t &hoid) const __attribute__((noinline));
  // True if hoid belongs in this mapping based on mask_bits and match
  bool check(const hobject_t &hoid) const {
    if (hoid.match(mask_bits, match)) {
      return true;
    }
    // if reached here - we need to report an error
    return report_check_error(hoid);
  }

  GlobalSnapMapper*     gsnap_ref;
  uint32_t              hash_prefix;
  uint32_t              hash_prefix_reversed;
  spg_t                 pgid;
  uint32_t              mask_bits;
  const uint32_t        match;
  const int64_t         pool;
  const SnapMapperShard shard;
  const std::string     shard_prefix;
  bool                  is_disabled = false;
};

#endif
