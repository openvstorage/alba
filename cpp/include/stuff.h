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

#ifndef STUFF_H
#define STUFF_H

#include <stdint.h>
#include <sys/stat.h>
#include <string>
#include <iostream>
#include <vector>

namespace alba {
namespace stuff {

uint64_t get_file_size(const std::string &file_name);

uint8_t unhex(char c);

void dump_buffer(std::ostream &os, const char *row, int size);
void dump_hex(std::ostream &os, unsigned char c);

template <typename T>
std::ostream &operator<<(std::ostream &os, const std::vector<T> &ts) {
  os << "[";
  for (auto &t : ts) {
    os << t;
    os << "; ";
  };
  os << "]";
  return os;
}
}
}
#endif
