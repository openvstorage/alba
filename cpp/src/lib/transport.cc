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

#include "transport.h"

namespace alba {
namespace transport {

std::ostream &operator<<(std::ostream &os, Kind t) {
  switch (t) {
  case Kind::tcp:
    os << "TCP";
    break;
  case Kind::rdma:
    os << "RDMA";
    break;
  }

  return os;
}

std::istream &operator>>(std::istream &is, Kind &t) {
  std::string s;
  is >> s;
  if (s == "TCP") {
    t = Kind::tcp;
  } else if (s == "RDMA") {
    t = Kind::rdma;
  } else {
    is.setstate(std::ios_base::failbit);
  }

  return is;
}

llio::message Transport::read_message() {
    return llio::message::from_reader([&](char* buffer, const int len) -> void {
          this -> read_exact(buffer,len);
      });
  /*
  uint32_t size;
  this->read_exact((char *)&size, 4);
  std::vector<char> buffer(size);
  this->read_exact(buffer.data(), size);
  return llio::message(buffer);
  */
}

void Transport::output(llio::message_builder &mb) {
  mb.output_using([&](const char *buffer, const int len) -> void {
    this->write_exact(buffer, len);
  });
}
}
}
