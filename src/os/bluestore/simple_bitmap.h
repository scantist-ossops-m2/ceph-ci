// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <benhanokh@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#pragma once
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <cmath>
#include <cassert>
#include <iomanip>

#include "include/ceph_assert.h"

struct extent_t {
  uint64_t offset;
  uint64_t length;
};

class SimpleBitmap {
public:
  typedef uint64_t bitmap_word_t;

  //----------------------------------------------------------------------------
  friend std::ostream& operator<<(std::ostream& os, const SimpleBitmap& bmap);

  //----------------------------------------------------------------------------
  //throw bad_alloc
  SimpleBitmap(CephContext *_cct, const std::string &_path, uint64_t num_bits);

  //----------------------------------------------------------------------------
  ~SimpleBitmap();
  
  // set a bit range range of @length starting at @offset
  bool     set(uint64_t offset, uint64_t length);
  // clear a bit range range of @length starting at @offset
  bool     clr(uint64_t offset, uint64_t length);

  // resets iterator to the next set extent
  void     start_itration_for_set_extents();
  // returns a copy of the next set extent and increments the iterator
  extent_t get_next_set_extent();

  // resets iterator to the next clear extent
  void     start_itration_for_clr_extents();
  // returns a copy of the next clear extent and increments the iterator
  extent_t get_next_clr_extent();  

  //----------------------------------------------------------------------------
  // clears all bits in the bitmap
  inline void clear_all() {
    std::memset(m_arr, 0, m_word_count * BYTES_IN_WORD);
  }

  //----------------------------------------------------------------------------
  // sets all bits in the bitmap
  inline void set_all() {
    std::memset(m_arr, 0xFF, m_word_count * BYTES_IN_WORD);
  }

private:
  //---------------------------------------------------------------------------
  static inline uint64_t offset_to_index(uint64_t offset) {
    return offset >> BITS_IN_WORD_SHIFT;
  }

  //---------------------------------------------------------------------------
  static inline uint64_t index_to_offset(uint64_t index) {
    return index << BITS_IN_WORD_SHIFT;
  }

  //---------------------------------------------------------------------------
  static  inline uint64_t bits_to_words(uint64_t bit_count) {
    return bit_count >> BITS_IN_WORD_SHIFT;
  }

  //---------------------------------------------------------------------------
  static  inline uint64_t words_to_bits(uint64_t words_count) {
    return words_count << BITS_IN_WORD_SHIFT;
  }
  
  constexpr static unsigned      BYTES_IN_WORD      = sizeof(bitmap_word_t);
  // assert that BYTES_IN_WORD is a power of 2 value
  static_assert ((BYTES_IN_WORD & (BYTES_IN_WORD - 1)) == 0);
  constexpr static unsigned      BITS_IN_WORD       = (BYTES_IN_WORD * 8);
  constexpr static unsigned      BITS_IN_WORD_SHIFT = std::log2(BITS_IN_WORD);
  constexpr static bitmap_word_t FULL_MASK          = (~((bitmap_word_t)0));

  CephContext*      cct;
  const std::string path;
  bitmap_word_t *m_arr;
  uint64_t       m_num_bits;
  uint64_t       m_word_count;
  uint64_t       m_word_idx_set;
  uint64_t       m_word_idx_clr;
  uint64_t       m_itr_word_set;
  uint64_t       m_itr_word_clr;
  
};

