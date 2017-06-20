/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#include "io.h"

namespace alba {
using namespace std;
void write_bool(ostream &os, const bool &b) {
  char c = b ? '\01' : '\00';
  os << c;
}

void read_bool(istream &is, bool &b) {
  char c = is.get();

  switch (c) {
  case '\01':
    b = true;
    break;
  case '\00':
    b = false;
    break;
  default: {
    ALBA_LOG(WARNING, "c:" << (uint)c << " is not a bool")
    throw alba::llio::deserialisation_exception("read_bool");
  }
  };
}

template <> void write_x<bool>(std::ostream &os, const bool &b) {
  write_bool(os, b);
}
template <> void read_x<bool>(std::istream &is, bool &b) { read_bool(is, b); }

template <> void write_x<uint32_t>(ostream &os, const uint32_t &i) {
  const char *ip = (const char *)(&i);
  os.write(ip, 4);
}

template <> void read_x<uint32_t>(istream &is, uint32_t &i) {
  is.read((char *)&i, 4);
  ALBA_LOG(DEBUG, "read_x<uint32_t>: i= " << i)
}

template <> void write_x<uint64_t>(ostream &os, const uint64_t &i) {
  const char *ip = (const char *)(&i);
  os.write(ip, 8);
}

template <> void read_x<uint64_t>(istream &is, uint64_t &i) {
  is.read((char *)&i, 8);
  ALBA_LOG(DEBUG, "read_x<uint64_t>: i = " << i)
}

template <> void write_x<string>(ostream &os, const string &s) {
  uint32_t size = s.size();
  write_x<uint32_t>(os, size);
  os << s;
}

template <> void read_x<string>(istream &is, string &s) {
  uint32_t size;
  read_x<uint32_t>(is, size);
  char *data = new char[size];
  is.read(data, size);
  s.assign(data, size);
  delete[] data;
}
}
