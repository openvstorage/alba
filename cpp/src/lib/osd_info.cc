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
#include "osd_info.h"
#include "alba_logger.h"
#include "llio.h"
#include "stuff.h"
#include <iostream>

namespace alba {
namespace llio {

template <> void from(message &m, proxy_protocol::OsdInfo &info) {
  uint8_t version;
  from(m, version);
  if (version != 3) {
    throw deserialisation_exception(
        "unexpected version while deserializing OsdInfo");
  }
  std::string inner_s;
  from(m, inner_s);
  auto mb = message_buffer::from_string(inner_s);
  std::vector<char> inner_v(inner_s.begin(), inner_s.end());
  message inner(mb);

  uint8_t kind;
  from(inner, kind);
  if (kind != 1) {
    info.kind_asd = false;
    return;
  }
  info.kind_asd = true;
  from(inner, info.ips);
  from(inner, info.port);
  from(inner, info.use_tls);
  from(inner, info.use_rdma);
  from(inner, info.long_id);
  from(inner, info.node_id);
  /*
TODO could someday also deserialize the following properties (if they would be
useful here):
decommissioned
other
total
used
seen
read
write
errors
checksum_errors
*/
}

template <> void from(message &m, proxy_protocol::OsdCapabilities &caps) {
  uint32_t size;
  from(m, size);
  for (uint i = 0; i < size; i++) {
    uint8_t version;
    from(m, version);

    uint32_t length;
    if (version == 1) {
      length = 0;
    } else if (version == 2) {
      from(m, length);
    } else {
      throw deserialisation_exception(
          "unexpected version while deserializing OsdCapabilities");
    }
    uint32_t pos0 = m.get_pos();

    uint8_t tag;
    from(m, tag);
    switch (tag) {
    case 1: {
    }; break; // for now, we don't care
    case 2: {
    }; break;
    case 3: {
      uint32_t port;
      from(m, port);
      caps.rora_port.emplace(port);
    }; break;
    case 4: {
      std::vector<std::string> rora_ips;
      from(m, rora_ips);
      caps.rora_ips.emplace(rora_ips);
      uint32_t port;
      from(m, port);
      caps.rora_port.emplace(port);
      std::string transport;
      from(m, transport);
      caps.rora_transport.emplace(transport);
    }
    default: {
      if (length == 0) {
        throw deserialisation_exception("OsdCapabilities");
      }
    };
    }

    if (length != 0) {
      uint32_t pos1 = m.get_pos();
      uint32_t actual_length = pos1 - pos0;
      if (actual_length > length) {
        throw deserialisation_exception("OsdCapabilities");
      } else if (actual_length < length) {
        m.skip(length - actual_length);
      }
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
  using alba::stuff::operator<<;
  os << "OsdCapabilities( rora_port= " << caps.rora_port
     << ", rora_transport= " << caps.rora_transport
     << ", rora_ips=" << caps.rora_ips << ")";
  return os;
}
}
}
