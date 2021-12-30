// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_BITMAPFASTALLOCATOR_H
#define CEPH_OS_BLUESTORE_BITMAPFASTALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "os/bluestore/bluestore_types.h"
#include "fastbmap_allocator_impl.h"
#include "include/mempool.h"
#include "common/debug.h"

class BitmapAllocator : public Allocator,
  public AllocatorLevel02<AllocatorLevel01Loose> {
  CephContext* cct;
public:
  BitmapAllocator(CephContext* _cct, int64_t capacity, int64_t alloc_unit,
		  std::string_view name);
  ~BitmapAllocator() override
  {
  }

  const char* get_type() const override
  {
    return "bitmap";
  }
  int64_t allocate(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector *extents) override;

  void release(
    const interval_set<uint64_t>& release_set) override;

  using Allocator::release;

  uint64_t get_free() override
  {
    return get_available();
  }

  void dump() override;
  void dump(std::function<void(uint64_t offset, uint64_t length)> notify) override;
  double get_fragmentation() override
  {
    return _get_fragmentation();
  }

  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override
  {
    _init_rm_free(offset, length);
  }

  // This API is identical to the init_rm_free() above with one difference -
  // it allows marking the same space as allocated multiple times and won't assert
  // It is used only by the recovery code when building the allocation map.
  void init_rm_free_allow_duplication(uint64_t offset, uint64_t length) {
    uint64_t allocated = _init_rm_free(offset, length);
    available += allocated;
  }

  void shutdown() override;
private:
  uint64_t _init_rm_free(uint64_t offset, uint64_t length);
};

#endif
