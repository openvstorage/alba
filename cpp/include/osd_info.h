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

#pragma once
#include "alba_common.h"
#include <boost/optional.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace alba {
namespace proxy_protocol {

struct OsdInfo {
  std::string long_id;
  std::vector<std::string> ips;
  uint32_t port;
  bool use_tls;
  bool use_rdma;
  std::string node_id;
};

struct OsdCapabilities {
  boost::optional<uint32_t> rora_port = boost::none;
  boost::optional<std::string> rora_transport = boost::none;
  boost::optional<std::vector<std::string>> rora_ips = boost::none;
};

typedef std::pair<OsdInfo, OsdCapabilities> info_caps;
typedef std::map<osd_t, std::shared_ptr<info_caps>> osd_map_t;
typedef std::vector<std::pair<alba_id_t, osd_map_t>> osd_maps_t;

std::ostream &operator<<(std::ostream &, const OsdInfo &);
std::ostream &operator<<(std::ostream &, const OsdCapabilities &);
}
}
