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

#include "GlobalSnapMapper.h"
#include <fmt/printf.h>

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "snap_mapper."

using std::map;
using std::set;
using std::string;
using std::vector;

const static std::string MAPPING_PREFIX = "SNA_";
const static std::string OBJECT_PREFIX  = "OBJ_";

void GlobalSnapMapper::object_snaps::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(oid, bl);
  encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void GlobalSnapMapper::object_snaps::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(oid, bl);
  decode(snaps, bl);
  DECODE_FINISH(bl);
}

//----------------------------------------------------
uint32_t GlobalSnapMapper::count_objects() const
{
  uint32_t count = 0;
  std::shared_lock rd_lock(m_mutex); // <<<<<<
  for (auto snap_to_objs : snap_to_objs_arr) {
    if (snap_to_objs == nullptr) {
      continue;
    }

    for (auto itr = snap_to_objs->begin(); itr != snap_to_objs->end(); ++itr) {
      // can access obj_set counter without a lock
      auto & objs = itr->second;

      // take a Full Lock on the obj_set
      std::unique_lock lock(objs.lock); // <<<<<<
      count += objs.set.size();
    }
  }
  return count;
}

//----------------------------------------------------
void GlobalSnapMapper::print_snaps(const char *s) const
{
  std::shared_lock rd_lock(m_mutex); // <<<<<<
  for (auto snap_to_objs : snap_to_objs_arr) {
    if (snap_to_objs == nullptr) {
      continue;
    }

    for (auto itr = snap_to_objs->begin(); itr != snap_to_objs->end(); ++itr) {
      dout(1) << "PRN::GBH::SNAPMAP:: called from: [" << s << "] snap_id=" << itr->first << dendl;
      auto & objs = itr->second;
      // take a Full Lock on the obj_set
      std::unique_lock lock(objs.lock); // <<<<<<
      for (const hobject_t& coid : objs.set) {
	dout(1) << "PRN::GBH::SNAPMAP:: [" << itr->first << "] --> [" << coid << "]" << dendl;
      }
      dout(1) << "=========================================" << dendl;
    }
  }
}

//----------------------------------------------------
// an inefficent implementation, but it is only called a few time at shutdown
SnapMapperShard GlobalSnapMapper::get_snap_mapper_shard(const snap_to_objs_map_t* snap_to_objs) const
{
  uint32_t idx = 0;
  std::shared_lock rd_lock(m_mutex); // <<<<<<
  for (auto snap_to_objs : snap_to_objs_arr) {
    if (snap_to_objs_arr[idx] == snap_to_objs) {
      SnapMapperShard shard;
      shard.set_id(idx);
      return shard;
    }
    idx++;
  }
  // should never arrive here
  ceph_abort("illegal snap_to_objs");
  return SnapMapperShard(shard_id_t(ILLEGAL_SM_SHARD_ID));
}

//----------------------------------------------------
bool GlobalSnapMapper::check(const hobject_t &hoid, uint32_t mask_bits, uint32_t match) const
{
  if (hoid.match(mask_bits, match)) {
    return true;
  }
  derr << __func__ << " " << hoid << " mask_bits " << mask_bits
       << " match 0x" << std::hex << match << std::dec << " is false"
       << dendl;
  return false;
}

//--------------------------------------------
void GlobalSnapMapper::add_oid(SnapMapperShard shard, const hobject_t & coid, const std::vector<snapid_t>& snaps)
{
  dout(20) << "GBH::SNAPMAP::" << __func__ << "::shard=" << shard << "(" << coid << ") -> (" << snaps << ")" << dendl;
  ceph_assert(!snaps.empty());

  std::vector<snapid_t> deferred_snaps;
  // Attempt to add oid to all obj_sets using a shared-lock on the snap_to_objs map
  // If this is the first coid added to the snap -> we will need to create an obj_set under write-lock
  std::shared_lock rd_lock(m_mutex); // <<<<<<
  auto snap_to_objs = get_snap_to_objs(shard);
  if (likely(snap_to_objs != nullptr)) {
    // add the coid to the coid_set of all snaps affected by it
    for (const snapid_t & snapid : snaps) {
      dout(20) << "+++GBH::SNAPMAP::" << __func__ << "::(" << snapid << ") -> (" << coid << ")" <<  dendl;

      auto itr = snap_to_objs->find(snapid);
      if (itr != snap_to_objs->end()) {
	// TBD:: then take a Full Lock on the  obj_set
	auto & objs = itr->second;
	std::unique_lock lock(objs.lock); // <<<<<<
	objs.set.insert(coid);
      }
      else {
	// We need to add the obj_set to the map first under write-lock
	// will do that later
	deferred_snaps.emplace_back(snapid);
      }
    }
  }
  else {
    // This is the first time we add entries to this shard
    // push all snaps to deferred_snaps to be processed later under wrt-lock
    deferred_snaps = snaps;
  }
  rd_lock.unlock(); // >>>>
  ///TBD delagte the read_lock to the write_lock

  if (unlikely(!deferred_snaps.empty())) {
    std::unique_lock wrt_lock(m_mutex); // !!<<<<<<
    // first check if the shard exists and if not -> create it!
    if (unlikely(snap_to_objs == nullptr)) {
      dout(10) << "GBH::SNAPMAP::" << "::create_snap_to_objs()" <<  dendl;
      snap_to_objs = create_snap_to_objs(shard);
    }

    for (const snapid_t & snapid : deferred_snaps) {
      // no need to lock objs_Set since we hold wrt_lock on the whole map
      (*snap_to_objs)[snapid].set.insert(coid);
      dout(10) << "+GBH::SNAPMAP::" << __func__ << "::(" << snapid << ") -> (" << coid << ")" <<  dendl;
    }
  }

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : snaps) {
      dout(20) << __func__ << " set " << i << dendl;
    }
  }
}

//---------------------------------------------------------------------------
void GlobalSnapMapper::add_oid(SnapMapperShard shard, const hobject_t & coid, snapid_t snapid)
{
  dout(20) << "GBH::SNAPMAP::" << __func__ << "::shard=" << shard << "(" << coid << ") -> (" << snapid << ")" << dendl;
  std::unique_lock wrt_lock(m_mutex); // !!<<<<<<
  auto snap_to_objs = get_snap_to_objs(shard);
  if (unlikely(snap_to_objs == nullptr)) {
    dout(10) << "GBH::SNAPMAP::" << "::create_snap_to_objs()" <<  dendl;
    snap_to_objs = create_snap_to_objs(shard);
  }

  (*snap_to_objs)[snapid].set.insert(coid);
}

//---------------------------------------------------------------------------
int GlobalSnapMapper::delete_pg_snap(
  snap_to_objs_map_t *snap_to_objs,
  SnapMapperShard     shard,
  int64_t             pool,
  const spg_t        &pgid,
  uint32_t            hash_prefix,
  uint32_t            hash_prefix_reversed,
  uint32_t            mask_bits,
  uint32_t            match,
  snapid_t            snapid)
{
  const uint32_t hash_mask = ~( (uint32_t)(~0) >> mask_bits);
  hobject_t      start_obj(snapid, hash_prefix, false, pool);
  uint64_t       count = 0;
  const uint64_t MAX_DELETE =  1024;
  bool           has_more = true;

  while (has_more) {
    // first take a shared-lock on the snap_to_objs map
    std::shared_lock rd_lock(m_mutex); // <<<<<<
    auto itr = snap_to_objs->find(snapid);
    if (itr != snap_to_objs->end()) {
      auto & objs = itr->second;
      // then take a Full Lock on the  obj_set
      std::unique_lock lock(objs.lock); // <<<<<<
      auto start_itr = objs.set.lower_bound(start_obj);
      for (auto itr = start_itr; itr != objs.set.end(); itr++, count++) {
	if (unlikely( (itr->pool != pool) || ((itr->get_bitwise_key() & hash_mask) != hash_prefix_reversed) ) ) {
	  has_more = false;
	  objs.set.erase(start_itr, itr);
	  break;
	}
	if (count % MAX_DELETE == 0) {
	  ++itr;
	  objs.set.erase(start_itr, itr);
	  // release the lock and give others a chance to work
	  lock.unlock();    // >>>>>>
	  rd_lock.unlock(); // >>>>>>
	  std::this_thread::yield();
	}
      }
    }
    else {
      rd_lock.unlock(); // >>>>>>
      // There is no mapping from @snap on the system (should not happen normally as the system did start yet)
      // It could happen in the future if this function is called for a live system since we release locks between snapids
      derr << "GBH::SNAPMAP::" << __func__ << "::There is no mapping for snap " << snapid << " (-ENOENT)" << dendl;
      return -ENOENT;
    }
  }

  dout(1) << "GBH::SNAPMAP::" << __func__ << "::Snapid " << snapid << " was successfully deleted, count="<< count << dendl;
  return 0;
}

//---------------------------------------
int GlobalSnapMapper::delete_pg(
  SnapMapperShard    shard,
  int64_t            pool,
  const spg_t       &pgid,
  uint32_t           hash_prefix,
  uint32_t           hash_prefix_reversed,
  uint32_t           mask_bits,
  uint32_t           match)
{
  // first create a vector with all exuisting snap-ids
  std::vector<snapid_t> snaps_vec;
  std::shared_lock rd_lock(m_mutex); // <<<<<<

  auto snap_to_objs = get_snap_to_objs(shard);
  if (snap_to_objs == nullptr || snap_to_objs->empty()) {
    rd_lock.unlock(); // >>>>>>
    dout(10) << "GBH::SNAPMAP::" << __func__ << "::There is no mapping for shard " << shard << " (-ENOENT)" << dendl;
    return 0;
  }

  for (const auto& [snapid, objs] : *snap_to_objs) {
    snaps_vec.push_back(snapid);
  }
  rd_lock.unlock(); // >>>>>>

  int ret = 0;
  for (snapid_t snapid : snaps_vec) {
    ret |= delete_pg_snap(snap_to_objs, shard, pool, pgid, hash_prefix, hash_prefix_reversed, mask_bits, match, snapid);
  }

  return ret;
}

//---------------------------------------
int GlobalSnapMapper::get_next_objects_to_trim(
  SnapMapperShard    shard,
  int64_t            pool,
  const spg_t       &pgid,
  uint32_t           hash_prefix,
  uint32_t           hash_prefix_reversed,
  uint32_t           mask_bits,
  uint32_t           match,
  snapid_t           snapid,
  unsigned           max,
  vector<hobject_t> *out)
{
  ceph_assert(out);
  ceph_assert(out->empty());
  ceph_assert(max > 0);

  const uint32_t hash_mask = ~( (uint32_t)(~0) >> mask_bits);
  hobject_t start_obj(snapid, hash_prefix, false, pool);

  // first take a shared-lock on the snap_to_objs map
  std::shared_lock rd_lock(m_mutex); // <<<<<<
  auto snap_to_objs = get_snap_to_objs(shard);
  if (unlikely(snap_to_objs == nullptr)) {
    rd_lock.unlock(); // >>>>>>
    // There is no mapping for @shard on the system
    dout(10) << "GBH::SNAPMAP::" << __func__ << "::There is no mapping for shard (-ENOENT)" << dendl;
    return -ENOENT;
  }

  auto itr = snap_to_objs->find(snapid);
  if (itr != snap_to_objs->end()) {
    auto & objs = itr->second;
    // then take a Full Lock on the  obj_set
    std::unique_lock lock(objs.lock); // <<<<<<

    auto start_itr = objs.set.lower_bound(start_obj);
    auto itr = start_itr;
    for ( ; itr != objs.set.end(); itr++) {
      if( unlikely( (itr->pool != pool) || ((itr->get_bitwise_key() & hash_mask) != hash_prefix_reversed) ) ) {
	break;
      }
      const hobject_t & coid = *itr;
      //dout(20) << "GBH::SNAPMAP::" << __func__ << "::shard=" << shard << ", snapid=" << snapid << "-->" << coid << dendl;
      ceph_assert(check(coid, mask_bits, match));
      out->push_back(coid);
      if (out->size() == max) {
	++itr;
	//objs.set.erase(start_itr, itr);
	objs.lock.unlock();      // >>>>>>
	rd_lock.unlock(); // >>>>>>
	dout(20) << "GBH::SNAPMAP::" << __func__ << "::got max objects!!" << dendl;
	return 0;
      }
    }
  }
  else {
    rd_lock.unlock(); // >>>>>>
    // There is no mapping from @snap on the system
    dout(10) << "GBH::SNAPMAP::" << __func__ << "::There is no mapping for snap (-ENOENT)" << dendl;
    return -ENOENT;
  }

  rd_lock.unlock(); // >>>>>>

  if (out->size() == 0) {
    if (pgid.pgid.m_pool == 1 &&  pgid.pgid.m_seed == 3) {
      dout(10) << "GBH::SNAPMAP::" << __func__ << "::No Objects were found (-ENOENT)" << dendl;
    }
    return -ENOENT;
  } else {
    dout(10) << "GBH::SNAPMAP::" << __func__ << "::got " << out->size() << " objects!!" << dendl;
    return 0;
  }
}

//----------------------------------------------------
int GlobalSnapMapper::remove_mapping_from_snapid_to_hobject(
  SnapMapperShard  shard,
  const hobject_t &coid,
  const snapid_t  &snapid)
{
  dout(20) << "GBH::SNAPMAP::" << __func__ << "::shard=" << shard << "snapid=" << snapid << dendl ;

  // first take a shared-lock on the snap_to_objs map
  std::shared_lock rd_lock(m_mutex); // <<<<<<
  auto snap_to_objs = get_snap_to_objs(shard);
  ceph_assert(snap_to_objs);

  // remove the coid from the coid_set of objects modified since snap creation
  auto itr = snap_to_objs->find(snapid);
  if (itr != snap_to_objs->end()) {
    // then take a Full Lock on the obj_set
    auto & objs = itr->second;
    std::unique_lock lock(objs.lock); // <<<<<<
    ceph_assert(objs.set.erase(coid) == 1);
    // if was the last element in the set -> remove the mapping
    if (unlikely(objs.set.empty())) {
      // release obj_set lock
      lock.unlock();
      // relase shared-lock and take a write-lock
      rd_lock.unlock();
      dout(20) << "GBH::SNAPMAP::" << __func__ << "::removed the last obj from snap " << snapid << dendl;
      std::unique_lock wrt_lock(m_mutex); // <<<<<<
      // check again under write-lock
      itr = snap_to_objs->find(snapid);
      if (itr != snap_to_objs->end() && itr->second.set.empty()) {
	snap_to_objs->erase(snapid);
      }
      else {
	wrt_lock.unlock();
	dout(1) << "GBH::SNAPMAP::" << __func__ << "::Failed to remove the last obj from snap " << snapid << dendl;
      }
      // should we return -ENOENT here ???
    }
    return 0;
  }
  else {
    derr << __func__ << "::GBH::SNAPMAP::coid=" << coid << " is mapped to snapid=" << snapid
	 << " , but reverse mapping doesn't exist (-EINVAL)"<< dendl;
    ceph_assert(0);
    return -EINVAL;
  }
}

//---------------------------------------------------------
int GlobalSnapMapper::update_snaps(
  SnapMapperShard         shard,
  const hobject_t        &coid,
  const vector<snapid_t> &new_snaps,
  const vector<snapid_t> &old_snaps
  /*MapCacher::Transaction<std::string, bufferlist> *t*/)
{
  dout(20) << "GBH::SNAPMAP::" << __func__ << "shard=" << shard << " (" << coid << ") new_snaps = " << new_snaps << ", old_snaps = " << old_snaps << dendl;
  if (new_snaps.empty())
    return _remove_oid(shard, coid, old_snaps);

  // remove the @coid from all coid_sets of snaps it no longer belongs to
  // (probably snaps are in the process of being removed)
  for (const snapid_t & snapid : old_snaps) {
    if (std::find(new_snaps.begin(), new_snaps.end(), snapid) == new_snaps.end()) {
      dout(20) << "---GBH::SNAPMAP::" << __func__ << "::remove mapping from snapid->obj_id :: " << snapid << "::" << coid << dendl;
      remove_mapping_from_snapid_to_hobject(shard, coid, snapid);
      if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
	dout(20) << __func__ << " rm " << coid << " from " << snapid << dendl;
      }
    }
  }

  return 0;
}

//---------------------------------------------------------------------------
int GlobalSnapMapper::_remove_oid(SnapMapperShard              shard,
				  const hobject_t             &coid,
				  const std::vector<snapid_t> &old_snaps)
{
  dout(20) << "GBH::SNAPMAP::" << __func__ << "::shard=" << shard << "::coid=" << coid << "::<" << old_snaps << ">" << dendl;

  // iterate over snap-set attached to this coid
  for (const snapid_t & snapid : old_snaps) {
    // remove the coid from the coid_set of objects modified since snap creation
    dout(20) << "---GBH::SNAPMAP::" << __func__ << "::remove mapping from snapid->obj_id :: " << snapid << "::" << coid << dendl;
    remove_mapping_from_snapid_to_hobject(shard, coid, snapid);

    if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
      dout(20) << __func__ << " rm " << coid << " from " << snapid << dendl;
    }
  }

  return 0;
}

//---------------------------------------------------------------------
int GlobalSnapMapper::get_snaps_for_scrubber(SnapMapperShard     shard,
					     const hobject_t    &coid,
					     std::set<snapid_t> &out)
{
  std::shared_lock rd_lock(m_mutex); // <<<<<<

  auto snap_to_objs = get_snap_to_objs(shard);
  if (likely(snap_to_objs != nullptr)) {
    for (auto itr = snap_to_objs->begin(); itr != snap_to_objs->end(); ++itr) {
      auto & objs = itr->second;
      // take a Full Lock on the obj_set
      std::unique_lock lock(objs.lock); // <<<<<<
      if (objs.set.contains(coid)) {
	out.insert(itr->first);
      }
    }
    return 0;
  }
  else {
    return -1;
  }
}
