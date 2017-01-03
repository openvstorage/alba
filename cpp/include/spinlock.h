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

#pragma once

#include <pthread.h>
#include <string>

// nicked and adapted from volumedriver/src/youtils/SpinLock.h

namespace alba {
namespace spinlock {
struct spinlock_exception : std::exception {
  spinlock_exception(std::string what) : _what(what) {}

  std::string _what;

  virtual const char *what() const noexcept { return _what.c_str(); }
};

class SpinLock

{
public:
  SpinLock();

  SpinLock(const SpinLock &) = delete;
  SpinLock &operator=(const SpinLock &) = delete;

  ~SpinLock();

  void lock();

  bool tryLock();

  void unlock();

  void assertLocked();

private:
  pthread_spinlock_t lock_;
};

class ScopedSpinLock {
public:
  explicit ScopedSpinLock(SpinLock &sl);

  ScopedSpinLock(const ScopedSpinLock &) = delete;
  ScopedSpinLock &operator=(const ScopedSpinLock &) = delete;

  ~ScopedSpinLock();

private:
  SpinLock &sl_;
};
}
}
