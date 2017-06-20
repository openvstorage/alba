/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once
#include "alba_common.h"
#include "asd_access.h"
#include "osd_info.h"
#include "proxy_client.h"
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace alba {
namespace proxy_client {

struct asd_slice {
  std::string key;
  uint32_t offset;
  uint32_t len;
  byte *target;
};

struct osd_access_exception : std::exception {
  osd_access_exception(uint32_t return_code, std::string what)
      : _return_code(return_code), _what(what) {}

  uint32_t _return_code;
  std::string _what;

  virtual const char *what() const noexcept { return _what.c_str(); }
};

using namespace proxy_protocol;

class OsdAccess {
public:
  static OsdAccess &getInstance(int connection_pool_size,
                                std::chrono::steady_clock::duration timeout);

  OsdAccess(OsdAccess const &) = delete;
  void operator=(OsdAccess const &) = delete;
  bool osd_is_unknown(osd_t);

  bool update(Proxy_client &client);

  int read_osds_slices(std::map<osd_t, std::vector<asd_slice>> &);

  std::vector<alba_id_t> get_alba_levels(Proxy_client &client);

private:
  OsdAccess(int connection_pool_size,
            std::chrono::steady_clock::duration timeout)
      : _connection_pool_size(connection_pool_size), _timeout(timeout),
        _filling(false) {}

  int _connection_pool_size;
  std::chrono::steady_clock::duration _timeout;

  std::mutex _osd_maps_mutex;
  osd_maps_t _osd_maps;
  std::vector<alba_id_t> _alba_levels; // TODO should invalidate some things
                                       // when last alba_level changes

  std::shared_ptr<info_caps> _find_osd(osd_t);

  int _read_osd_slices_asd_direct_path(osd_t osd,
                                       std::vector<asd_slice> &slices);
  asd::ConnectionPools asd_connection_pools;

  std::atomic<bool> _filling;
  std::mutex _filling_mutex;
  std::condition_variable _filling_cond;
};

std::ostream &operator<<(std::ostream &, const asd_slice &);
}
}
