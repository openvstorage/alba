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

#include "checksum.h"

namespace alba {

void Sha1::print(std::ostream &os) const {
  os << "Sha1(`";
  for (char c : _digest) {
    alba::stuff::dump_hex(os, c);
  }
  os << "`)";
}

void Crc32c::print(std::ostream &os) const {
  os << "Crc32c 0x";
  uint32_t d = _digest;
  using namespace alba::stuff;
  dump_hex(os, d >> 24);
  dump_hex(os, (d >> 16) & 0xff);
  dump_hex(os, (d >> 8) & 0xff);
  dump_hex(os, (d)&0xff);
}

bool verify(const Checksum &c0, const Checksum &c1) {
  algo_t a0 = c0.get_algo();
  algo_t a1 = c1.get_algo();
  if (a0 != a1) {
    return false;
  } else {
    switch (a0) {
    case algo_t::NO_CHECKSUM:
      return true;
    case algo_t::SHA1: {
      auto &s0 = (const Sha1 &)c0;
      auto &s1 = (const Sha1 &)c1;
      return (s0._digest == s1._digest);
    }
    case algo_t::CRC32c: {
      auto &s0 = (const Crc32c &)c0;
      auto &s1 = (const Crc32c &)c1;
      return (s0._digest == s1._digest);
    }
    }
  }
  // g++ issues bogus:
  // warning: control reaches end of non-void function [-Wreturn-type]
  return false;
}

std::ostream &operator<<(std::ostream &os, const algo_t &algo) {
  switch (algo) {
  case algo_t::NO_CHECKSUM:
    os << "NO_CHECKSUM";
    break;
  case algo_t::SHA1:
    os << "SHA1";
    break;
  case algo_t::CRC32c:
    os << "Crc32c";
    break;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, const Checksum &c) {
  c.print(os);
  return os;
}
}

namespace alba {
namespace llio {
template <> void from(message &m, alba::Checksum *&r) {
  uint8_t type;
  from(m, type);
  switch (type) {
  case 1: {
    r = new alba::NoChecksum();
  }; break;
  case 2: {
    std::string digest;
    from(m, digest);
    r = new alba::Sha1(digest);
  }; break;
  case 3: {
    uint32_t d;
    from(m, d);
    r = new alba::Crc32c(d);
  } break;
  default:
    throw "serialization error";
  }
}

template <> void from(message &m, std::unique_ptr<alba::Checksum> &p) {
  alba::Checksum *c;
  from(m, c);
  p.reset(c);
}

template <>
void to(message_builder &mb, alba::Checksum const *const &checksum) noexcept {
  checksum->to(mb);
}
}
}
