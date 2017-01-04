// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes in
// the LICENSE.txt file of the Open vStorage OSE distribution.
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.

#include "spinlock.h"
#include "alba_logger.h"

#include <cerrno>
#include <cstring>

// nicked and adapted from volumedriver/src/youtils/SpinLock.cpp

namespace alba {
namespace spinlock {

SpinLock::SpinLock() {
  int ret = ::pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
  if (ret) {
    ALBA_LOG(ERROR, "Failed to initialize spinlock " << &lock_ << ": "
                                                     << strerror(ret));
    throw spinlock_exception("Failed to initialize spinlock");
  }
}

SpinLock::~SpinLock() {
  int ret = pthread_spin_destroy(&lock_);
  if (ret) {
    ALBA_LOG(ERROR, "Failed to destroy spinlock " << &lock_ << ": "
                                                  << strerror(ret));
  }
}

void SpinLock::lock() {
  int ret = pthread_spin_lock(&lock_);
  if (ret) {
    ALBA_LOG(ERROR, "Failed to lock spinlock " << &lock_ << ": "
                                               << strerror(ret));
    throw spinlock_exception("Failed to lock spinlock");
  }
}

bool SpinLock::tryLock() {
  int ret = pthread_spin_trylock(&lock_);
  switch (ret) {
  case 0:
    return true;
  case EBUSY:
    return false;
  default:
    ALBA_LOG(ERROR, "Failed to trylock spinlock " << &lock_ << ": "
                                                  << strerror(ret));
    throw spinlock_exception("Failed to trylock spinlock");
  }
}

void SpinLock::assertLocked() { !tryLock(); }

void SpinLock::unlock() {
  int ret = pthread_spin_unlock(&lock_);
  if (ret) {
    ALBA_LOG(ERROR, "Failed to unlock spinlock " << &lock_ << ": "
                                                 << strerror(ret));
    throw spinlock_exception("Failed to unlock spinlock");
  }
}
}
}
