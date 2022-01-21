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

#include "simple_bitmap.h"

#include "include/ceph_assert.h"
#include "bluestore_types.h"
#include "common/debug.h"
#define dout_context cct
#define dout_subsys  ceph_subsys_bluestore

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << __func__ << "::SBMAP::" << this << " "

static struct extent_t null_extent = {0, 0};

//----------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& out, const SimpleBitmap& bmap)
{
  for (unsigned i = 0; i < bmap.m_word_count; i++) {
    if(bmap.m_arr[i] == 0) {
      continue;
    }

    out << "[" << i << "]" << "0x" << std::setfill('0') << std::setw(2) << std::right << std::hex;

    if (SimpleBitmap::BYTES_IN_WORD > 1) {
      out << bmap.m_arr[i] << std::endl;
    } else {
      // hex printout doesn't work for uint8_t, must convert to higher integer
      out << (unsigned) bmap.m_arr[i] << std::endl;
    }
  }
  return out;
}

//----------------------------------------------------------------------------
//throw bad_alloc
SimpleBitmap::SimpleBitmap(CephContext *_cct, const std::string &_path, uint64_t num_bits) :cct(_cct), path(_path)
{
  m_num_bits   = num_bits;
  m_word_count = bits_to_words(num_bits);
  if (num_bits & BITS_IN_WORD_MASK) {
    m_word_count++;
  }
  m_arr = new bitmap_word_t [m_word_count];
  clear_all();
  dout(20) << "m_word_count  = " << m_word_count  << dendl;
  dout(20) << "m_num_bits    = " << m_num_bits    << dendl;
  dout(20) << "BYTES_IN_WORD = " << BYTES_IN_WORD << dendl;
  dout(20) << "BITS_IN_WORD  = " << BITS_IN_WORD  << dendl;
}

//----------------------------------------------------------------------------
SimpleBitmap::~SimpleBitmap()
{
  dout(5) << __func__ << dendl;
  delete [] m_arr;
}

//----------------------------------------------------------------------------
bool SimpleBitmap::set(uint64_t offset, uint64_t length)
{
  dout(20) <<" [" << std::hex << offset << ", " << length << "]" << dendl;

  // special case optimization
  if (length == 1) {
    return set_single_bit(offset);
  }

  if (offset + length > m_num_bits) {
    derr << "offset + length = " << offset + length << " exceeds map size = " << m_num_bits << dendl;
    return false;
  }

  // find the first word index
  uint64_t index = offset_to_index(offset);
  // handle the first word which might be incomplete
  uint64_t first_bit_set = offset & BITS_IN_WORD_MASK;

  if (first_bit_set != 0) {
    bitmap_word_t set_mask      = FULL_MASK << first_bit_set;
    uint64_t      first_bit_clr = first_bit_set + length;
    if (first_bit_clr < BITS_IN_WORD) {
      uint64_t      clr_bits = BITS_IN_WORD - first_bit_clr;
      bitmap_word_t clr_mask = FULL_MASK >> clr_bits;
      set_mask     &= clr_mask;
      m_arr[index] |= set_mask;
      return true;
    } else {
      // set all bits in this word starting from first_bit_set
      m_arr[index] |= set_mask;
      index ++;
      length -= (BITS_IN_WORD - first_bit_set);
    }
  }

  // set a range of full words
  uint64_t full_words_count = bits_to_words(length);
  uint64_t end_range        = index + full_words_count;
  for ( ; index < end_range; index++ ) {
    m_arr[index] = FULL_MASK;
  }
  length -= words_to_bits(full_words_count);

  // set bits in the last word
  if (length) {
    bitmap_word_t set_mask = ~(FULL_MASK << length);
    m_arr[index] |= set_mask;
  }

  return true;
}

//----------------------------------------------------------------------------
bool SimpleBitmap::clr(uint64_t offset, uint64_t length)
{
  dout(20) <<" [" << std::hex << offset << ", " << length << "]" << dendl;
  // special case optimization
  if (length == 1) {
    return clr_single_bit(offset);
  }

  if (offset + length > m_num_bits) {
    derr << "offset + length = " << offset + length <<
      " exceeds map size = " << m_num_bits << dendl;
    ceph_assert(offset + length <= m_num_bits);
    return false;
  }

  // find the first word index
  uint64_t index = offset_to_index(offset);
  // handle the first word which might be incomplete
  uint64_t first_bit_clr = (offset & BITS_IN_WORD_MASK);
  if (first_bit_clr != 0) {
    uint64_t      clr_bits      = BITS_IN_WORD - first_bit_clr;
    bitmap_word_t clr_mask      = FULL_MASK >> clr_bits;
    uint64_t      first_bit_set = first_bit_clr + length;
    if (first_bit_set < BITS_IN_WORD) {
      uint64_t      set_mask = FULL_MASK << first_bit_set;
      clr_mask     |= set_mask;
      m_arr[index] &= clr_mask;
      return true;
    } else {
      // clear all bits in this word starting from first_bit_clr
      m_arr[index] &= clr_mask;
      index ++;
      length -= (BITS_IN_WORD - first_bit_clr);
    }
  }

  // set a range of full words
  uint64_t full_words_count = bits_to_words(length);
  uint64_t end_range        = index + full_words_count;
  for ( ; index < end_range; index++ ) {
    m_arr[index] = 0;
  }
  length -= words_to_bits(full_words_count);

  // set bits in the last word
  if (length) {
    bitmap_word_t clr_mask = (FULL_MASK << length);
    m_arr[index] &= clr_mask;
  }

  return true;
}

//----------------------------------------------------------------------------
extent_t SimpleBitmap::get_next_set_extent(uint64_t offset)
{
  dout(20) << "::offset =" << offset  << dendl;
  if (offset >= m_num_bits ) {
    dout(10) << "1)Reached the end of the bitmap" << dendl;
    return null_extent;
  }

  uint64_t word_idx = offset_to_index(offset);
  uint64_t word     = m_arr[word_idx];

  // clear all bit set before offset
  uint64_t bits_to_clear = (offset & BITS_IN_WORD_MASK);
  word >>= bits_to_clear;
  word <<= bits_to_clear;
  if (word == 0) {
      // skip past all clear words
    while (++word_idx < m_word_count && !m_arr[word_idx]);

    if (word_idx < m_word_count ) {
      word = m_arr[word_idx];
    } else {
      dout(10) << "2)Reached the end of the bitmap" << dendl;
      return null_extent;
    }
  }

  int           ffs = __builtin_ffsll(word);
  extent_t      ext;
  ext.offset = words_to_bits(word_idx) + (ffs - 1);

  // set all bits from current to LSB
  uint64_t      set_bits = BITS_IN_WORD - ffs;
  bitmap_word_t set_mask = FULL_MASK >> set_bits;
  word |= set_mask;

  // skipped past fully set words
  if (word == FULL_MASK) {
    while ( (++word_idx < m_word_count) && (m_arr[word_idx] == FULL_MASK) );

    if (word_idx < m_word_count) {
      word = m_arr[word_idx];
    } else {
      // bitmap is set from ext.offset until the last bit
      ext.length = (m_num_bits - ext.offset);
      return ext;
    }
  }

  ceph_assert(word != FULL_MASK);
  // reverse bits to allow ffs to be used
  word = ~word;

  // find the first clear bit (after reversing)
  int      ffz     = __builtin_ffsll(word);
  uint64_t zoffset = words_to_bits(word_idx) + (ffz - 1);
  ext.length = (zoffset - ext.offset);

  return ext;
}

//----------------------------------------------------------------------------
extent_t SimpleBitmap::get_next_clr_extent(uint64_t offset)
{
  dout(20) << "offset = " << offset << dendl;
  if (offset >= m_num_bits ) {
    dout(10) << "1)Reached the end of the bitmap" << dendl;
    return null_extent;
  }

  uint64_t word_idx = offset_to_index(offset);
  uint64_t word     = m_arr[word_idx];

  // set all bit set before offset
  offset &= BITS_IN_WORD_MASK;
  if (offset != 0) {
    uint64_t      bits_to_set = BITS_IN_WORD - offset;
    bitmap_word_t set_mask    = FULL_MASK >> bits_to_set;
      word |= set_mask;
  }
  if (word == FULL_MASK) {
    // skipped past fully set words
    while ( (++word_idx < m_word_count) && (m_arr[word_idx] == FULL_MASK) );

    if (word_idx < m_word_count) {
      word = m_arr[word_idx];
    } else {
      dout(10) << "2)Reached the end of the bitmap" << dendl;
      return null_extent;
    }
  }

  // reverse bits to allow ffs to be used
  word = ~word;
  ceph_assert(word);
  int      ffz = __builtin_ffsll(word);
  extent_t ext;
  ext.offset = words_to_bits(word_idx) + (ffz - 1);

  // reverse bit back to normal
  word = ~word;

  // clear all bits from current position to LSB
  word >>= ffz;
  word <<= ffz;

  // skip past all clear words
  if ( (word == 0) || (ffz == BITS_IN_WORD) ) {
    while ( (++word_idx < m_word_count) && (m_arr[word_idx] == 0) );

    if (word_idx < m_word_count) {
      word = m_arr[word_idx];
    } else {
      dout(10) << "bitmap is free from ext.offset until the last bit" << dendl;
      dout(10) << "m_num_bits=" << m_num_bits << " ext.offset=" << ext.offset << dendl;
      ext.length = (m_num_bits - ext.offset);
      return ext;
    }
  }

  int           ffs     = __builtin_ffsll(word);
  uint64_t      soffset = words_to_bits(word_idx) + (ffs - 1);
  ext.length = (soffset - ext.offset);
  return ext;
}
