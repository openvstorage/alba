/*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*/

#pragma once

#include "alba_logger.h"
#include "llio.h"
#include <iostream>
#include <memory>
#include <stdint.h>
#include <vector>

namespace alba {

template <typename T> void write_x(std::ostream &os, const T &t);

template <typename T> void read_x(std::istream &, T &t);

template <> void write_x<bool>(std::ostream &os, const bool &b);
template <> void read_x<bool>(std::istream &is, bool &b);

template <> void write_x<uint32_t>(std::ostream &os, const uint32_t &i);
template <> void read_x<uint32_t>(std::istream &is, uint32_t &i);

template <> void write_x<uint64_t>(std::ostream &os, const uint64_t &i);
template <> void read_x<uint64_t>(std::istream &is, uint64_t &i);

template <> void write_x<std::string>(std::ostream &os, const std::string &s);
template <> void read_x<std::string>(std::istream &is, std::string &s);

template <typename T> void write_x(std::ostream &os, const std::vector<T> &v) {
  ALBA_LOG(DEBUG, __PRETTY_FUNCTION__);
  uint32_t size = v.size();
  write_x(os, size);
  for (auto iter = v.rbegin(); iter != v.rend(); ++iter) {
    write_x(os, *iter);
  }
}

template <typename T> void read_vector(std::istream &is, std::vector<T> &v) {
  uint32_t size;
  read_x<uint32_t>(is, size);
  v.resize(size);
  ALBA_LOG(DEBUG, "read_vector (size= " << size << ")")
  for (int32_t i = size - 1; i >= 0; --i) {
    T e;
    read_x<T>(is, e);
    v[i] = e;
  }
}

template <typename T>
void write_x(std::ostream &os, const std::shared_ptr<T> &xp) {
  const T &x = *xp;
  write_x(os, x);
}
}
