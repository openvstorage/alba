/*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*/

#include "checksum.h"

namespace alba{

    void Sha1::print(std::ostream& os) const {
        os << "Sha1(`";
        for(char c: _digest){
          alba::stuff::dump_hex(os, c);
        }
        os << "`)";
    }

  void Crc32c::print(std::ostream& os) const {
    os << "Crc32c(`" << _digest << "`)";
  }

    bool verify(const Checksum& c0, const Checksum& c1){
        algo_t a0 = c0.get_algo();
        algo_t a1 = c1.get_algo();
        if(a0 != a1){
            return false;
        } else{
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
    }

    std::ostream& operator<<(std::ostream& os, const algo_t& algo){
        switch(algo){
        case algo_t :: NO_CHECKSUM: os << "NO_CHECKSUM"; break;
        case algo_t :: SHA1 : os << "SHA1"; break;
        case algo_t :: CRC32c : os << "CRC32c"; break;
        }
        return os;
    }

    std::ostream& operator<<(std::ostream& os, const Checksum& c){
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

template <>
void to(message_builder &mb, alba::Checksum const *const &checksum) noexcept {
  checksum->to(mb);
}
}
}
