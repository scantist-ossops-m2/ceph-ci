// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
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
#include <cstring>
#include <cmath>
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

  // returns a copy of the next set extent starting at @offset
  extent_t get_next_set_extent(uint64_t offset);

  // returns a copy of the next clear extent starting at @offset
  extent_t get_next_clr_extent(uint64_t offset);

  //----------------------------------------------------------------------------
  inline uint64_t get_size() {
    return m_num_bits;
  }

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

  //----------------------------------------------------------------------------
  bool bit_is_set(uint64_t offset) {
    if (offset < m_num_bits) {
      uint64_t      word_index = offset_to_index(offset);
      uint64_t      bit_offset = (offset & BITS_IN_WORD_MASK);
      bitmap_word_t mask       = 1ULL << bit_offset;
      return (m_arr[word_index] & mask);
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  bool bit_is_clr(uint64_t offset) {
    return (!bit_is_set(offset));
  }

  //----------------------------------------------------------------------------
  bool set_single_bit(uint64_t offset) {
    if (offset < m_num_bits) {
      uint64_t      word_index = offset_to_index(offset);
      uint64_t      bit_offset = (offset & BITS_IN_WORD_MASK);
      bitmap_word_t set_mask   = 1ULL << bit_offset;
      m_arr[word_index] |= set_mask;
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  bool clr_single_bit(uint64_t offset) {
    if (offset < m_num_bits) {
      uint64_t      word_index = offset_to_index(offset);
      uint64_t      bit_offset = (offset & BITS_IN_WORD_MASK);
      bitmap_word_t set_mask   = 1ULL << bit_offset;
      bitmap_word_t clr_mask   = ~set_mask;
      m_arr[word_index] &= clr_mask;
      return true;
    } else {
      return false;
    }
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
  constexpr static unsigned      BITS_IN_WORD_MASK  = (BITS_IN_WORD - 1);
  constexpr static unsigned      BITS_IN_WORD_SHIFT = std::log2(BITS_IN_WORD);
  constexpr static bitmap_word_t FULL_MASK          = (~((bitmap_word_t)0));

  CephContext*      cct;
  const std::string path;
  bitmap_word_t *m_arr;
  uint64_t       m_num_bits;
  uint64_t       m_word_count;
};
