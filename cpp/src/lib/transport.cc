/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
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
  auto mb = llio::message_buffer::from_reader(
      [&](char *buffer, const int len) -> void {
        this->read_exact(buffer, len);
      });
  return llio::message(mb);
}

void Transport::output(llio::message_builder &mb) {
  mb.output_using([&](const char *buffer, const int len) -> void {
    this->write_exact(buffer, len);
  });
}
}
}
