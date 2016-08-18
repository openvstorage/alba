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

#include <stdint.h>
#include <sys/stat.h>
#include <string>
#include <iostream>
#include <vector>
#include <memory>
#include <boost/optional.hpp>

namespace alba {
namespace stuff {

uint64_t get_file_size(const std::string &file_name);

uint8_t unhex(char c);

void dump_buffer(std::ostream &os, const char *row, int size);
void dump_hex(std::ostream &os, unsigned char c);

template <typename X>
std::ostream &operator<<(std::ostream &os, const std::shared_ptr<X> &xp) {
  const X &x = *xp;
  os << "&(" << x << ")";
  return os;
}

template <typename X>
std::ostream &operator<<(std::ostream &os, const std::unique_ptr<X> &xp) {
  const X &x = *xp;
  os << "!(" << x << ")";
  return os;
}

template <typename X, typename Y>
std::ostream &operator<<(std::ostream &os, const std::pair<X, Y> &p) {
  os << "(" << p.first << ", " << p.second << ")";
  return os;
}

template <typename X, typename Y, typename Z>
std::ostream &operator<<(std::ostream &os, const std::tuple<X, Y, Z> &p) {
  os << "(" << std::get<0>(p) << ", " << std::get<1>(p) << ", "
     << std::get<2>(p) << ")";
  return os;
}

template <typename T>
std::ostream &operator<<(std::ostream &os, const std::vector<T> &ts) {
  os << "{";
  for (auto it = begin(ts); it != end(ts); it++) {
    if (it != begin(ts)) {
      os << ", ";
    }
    os << *it;
  }
  os << "}";

  return os;
}

template <typename T>
std::ostream &operator<<(std::ostream &os, const boost::optional<T> &ot) {
  if (boost::none == ot) {
    os << "None";
  } else {
    os << "(Some " << *ot << ")";
  }
  return os;
}
}
}
