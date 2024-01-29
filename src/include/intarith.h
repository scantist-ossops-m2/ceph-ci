// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_INTARITH_H
#define CEPH_INTARITH_H

#include <bit>
#include <climits>
#include <concepts>
#include <type_traits>

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> div_round_up(T n, U d) {
  return (n + d - 1) / d;
}

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> round_down_to(T n, U d) {
  return n - n % d;
}

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> round_up_to(T n, U d) {
  return (n % d ? (n + d - n % d) : n);
}

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> shift_round_up(T x, U y) {
  return (x + (1 << y) - 1) >> y;
}

/*
 * Wrappers for various sorts of alignment and rounding.  The "align" must
 * be a power of 2.  Often times it is a block, sector, or page.
 */

/*
 * return x rounded down to an align boundary
 * eg, p2align(1200, 1024) == 1024 (1*align)
 * eg, p2align(1024, 1024) == 1024 (1*align)
 * eg, p2align(0x1234, 0x100) == 0x1200 (0x12*align)
 * eg, p2align(0x5600, 0x100) == 0x5600 (0x56*align)
 */
template<typename T>
constexpr inline T p2align(T x, T align) {
  return x & -align;
}

/*
 * return whether x is aligned with (1 << bits)
 * eg, p2aligned(1200, 10) ==> false
 * eg, p2align(1024, 10) ==> true
 * eg, p2align(0x1234, 8) ==> false
 * eg, p2align(0x5600, 8) ==> true
 */
template<typename T>
constexpr inline bool p2_isaligned(T x, unsigned bits) {
  return !(x & ((T(1) << bits) - 1));
}

/*
 * return x % (mod) align
 * eg, p2phase(0x1234, 0x100) == 0x34 (x-0x12*align)
 * eg, p2phase(0x5600, 0x100) == 0x00 (x-0x56*align)
 */
template<typename T>
constexpr inline T p2phase(T x, T align) {
  return x & (align - 1);
}

/*
 * return how much space is left in this block (but if it's perfectly
 * aligned, return 0).
 * eg, p2nphase(0x1234, 0x100) == 0xcc (0x13*align-x)
 * eg, p2nphase(0x5600, 0x100) == 0x00 (0x56*align-x)
 */
template<typename T>
constexpr inline T p2nphase(T x, T align) {
  return -x & (align - 1);
}

/*
 * return x rounded up to an align boundary
 * eg, p2roundup(0x1234, 0x100) == 0x1300 (0x13*align)
 * eg, p2roundup(0x5600, 0x100) == 0x5600 (0x56*align)
 */
template<typename T>
constexpr inline T p2roundup(T x, T align) {
  return -(-x & -align);
}

// count bits (set + any 0's that follow)
template<std::integral T>
unsigned cbits(T v) {
  return (sizeof(v) * CHAR_BIT) - std::countl_zero(std::make_unsigned_t<T>(v));
}

#endif
