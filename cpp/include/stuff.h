/*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
