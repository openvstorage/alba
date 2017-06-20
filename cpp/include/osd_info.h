/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
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
  bool kind_asd = false;
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
