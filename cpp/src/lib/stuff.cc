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

#include "stuff.h"
#include "alba_logger.h"
#include <exception>
#include <iomanip>
#include <string>
#include <sys/time.h>

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
  os.flags(flags);
}

void dump_data(std::ostream &os, char **rows, int k, int block_size) {
  for (int i = 0; i < k; i++) {
    char *start = rows[i];
    dump_buffer(os, start, block_size);
    os << std::endl;
  }
}

double timestamp_millis() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  double t0 = tp.tv_sec + (double)tp.tv_usec / 1e6;
  return t0;
}

std::string shell(const std::string &cmd) {
  ALBA_LOG(DEBUG, "stuff::shell: " << cmd);
  std::array<char, 128> buffer;
  std::string result;
  std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
  if (!pipe)
    throw std::runtime_error("popen() failed!");
  while (!feof(pipe.get())) {
    if (fgets(buffer.data(), 128, pipe.get()) != NULL)
      result += buffer.data();
  }
  return result;
}
}
}
