// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <errno.h>
#include "gtest/gtest.h"
#include "TestNewOpsClient.h"

TEST_F(TestNewOps, CheckDummyOP) {
  int res = client->check_dummy_op(myperm);
  ASSERT_EQ(res, -EOPNOTSUPP);
}

