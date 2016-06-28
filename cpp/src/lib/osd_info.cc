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
#include "osd_info.h"
#include "llio.h"
#include <iostream>
#include "alba_logger.h"
namespace alba {
namespace llio {
template <> void from(message &m, proxy_protocol::OsdInfo &info) {
  uint8_t version;
  from(m, version);
  assert(version == 3);
  std::string inner_s;
  from(m, inner_s);
  std::vector<char> inner_v(inner_s.begin(), inner_s.end());
  message inner(inner_v);

  uint8_t kind;
  from(inner, kind);
  assert(kind == 1);
  from(inner, info.ips);
  from(inner, info.port);
  from(inner, info.use_tls);
  from(inner, info.use_rdma);
}

template <> void from(message &m, proxy_protocol::OsdCapabilities &caps) {
  uint32_t size;
  from(m, size);
  for (uint i = 0; i < size; i++) {
    uint8_t version;
    from(m, version);
    assert(version == 1);
    uint8_t tag;
    from(m, tag);
    switch (tag) { case 1: { }; break; // for now, we don't care
    case 2: {
    }; break;
    case 3: {
      uint32_t port;
      from(m, port);
      caps.port.emplace(port);
    }; break;
    default: { throw "serialization error"; };
    }
  }
}
}
namespace proxy_protocol {
std::ostream &operator<<(std::ostream &os, const OsdInfo &info) {
  using alba::stuff::operator<<;
  os << "OsdInfo("
     << " port=" << info.port << ", ips= " << info.ips
     << ", use_tls=" << info.use_tls << ", use_rdma=" << info.use_rdma << ")";
  return os;
}

std::ostream &operator<<(std::ostream &os, const OsdCapabilities &caps) {
  os << "OsdCapabilities( port= ";

  if (boost::none == caps.port) {
    os << "None";
  } else {
    int port = *caps.port;
    os << "Some " << port;
  }

  os << ")";
  return os;
}
}
}
