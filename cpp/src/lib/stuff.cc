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

#include "stuff.h"
#include <exception>
#include <iomanip>

namespace alba {
namespace stuff {

uint64_t get_file_size(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  if (rc != 0) {
    std::exception e;
    throw e;
  }
  uint64_t size = stat_buf.st_size;
  return size;
}

uint8_t unhex(char c) {
  uint8_t r = (uint8_t)c;
  if ('0' <= c && c <= '9') {
    r -= 48;
  } else if ('a' <= c && c <= 'f') {
    r -= 87; // 'a' = 97
  } else {
    throw "unhex?";
  }
  return r;
}

int gcd(int a, int b) {
  int t;

  if (a > b) {
    t = b;
    b = a;
    a = t;
  }
  while (b != 0) {
    t = a % b;
    a = b;
    b = t;
  }

  return a;
}

int lcm(int a, int b) {
  int gcd_ = gcd(a, b);
  int lcm = (a * b) / gcd_;
  return lcm;
}

char hexlify(unsigned char c) {
  char r;
  if (c < 10) {
    r = c + 48;
  } else {
    r = c + 87;
  }
  return r;
}

void dump_hex(std::ostream &os, unsigned char c) {
  // yes unsigned (OMG)
  char h0 = hexlify(c >> 4);
  char h1 = hexlify(c & 0x0f);
  os << h0;
  os << h1;
}

void dump_buffer(std::ostream &os, const char *row, int size) {
  auto flags = os.flags();
  for (int j = 0; j < size; j++) {
    unsigned char c = (unsigned char)row[j];
    unsigned int ic = (unsigned int)c;
    os << std::setw(2) << std::setfill('0') << std::hex << ic << ' ';
  };
  for (int j = 0; j < size; j++) {
    unsigned char c = (unsigned char)row[j];
    unsigned char c2 = c;
    bool is_valid = (c2 >= '0') && (c2 <= '9');
    is_valid |= (c2 >= 'A' && c2 <= 'Z');
    is_valid = is_valid || (c2 >= 'a' && c2 <= 'z');
    is_valid |= c2 == ' ';
    if (!is_valid) {
      c2 = '.';
    };
    os << c2;
  };
  os.flags(flags);
}

void dump_data(std::ostream &os, char **rows, int k, int block_size) {
  for (int i = 0; i < k; i++) {
    char *start = rows[i];
    dump_buffer(std::cout, start, block_size);
    os << std::endl;
  }
}
}
}
