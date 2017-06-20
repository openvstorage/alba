/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/


#include "osd_access.h"
#include "alba_logger.h"

#include "stuff.h"
#include <assert.h>

namespace alba {
namespace proxy_client {

OsdAccess &OsdAccess::getInstance(int connection_pool_size,
                                  std::chrono::steady_clock::duration timeout) {
  static OsdAccess instance(connection_pool_size, timeout);
  return instance;
}

bool OsdAccess::osd_is_unknown(osd_t osd) {
  std::lock_guard<std::mutex> lock(_osd_maps_mutex);
  auto &pair = _osd_maps[_osd_maps.size() - 1];
  auto &map = pair.second;
  bool result = (map.find(osd) == map.end());
  return result;
}

std::shared_ptr<info_caps> OsdAccess::_find_osd(osd_t osd) {
  std::lock_guard<std::mutex> lock(_osd_maps_mutex);
  auto &pair = _osd_maps[_osd_maps.size() - 1];
  auto &map = pair.second;
  const auto &ic = map.find(osd);
  if (ic == map.end()) {
    return nullptr;
  } else {
    return ic->second;
  }
}

bool OsdAccess::update(Proxy_client &client) {
  bool result = true;
  if (!_filling.load()) {
    ALBA_LOG(INFO, "OsdAccess::update:: filling up");
    std::lock_guard<std::mutex> f_lock(_filling_mutex);
    if (!_filling.load()) {
      _filling.store(true);
      try {
        std::lock_guard<std::mutex> lock(_osd_maps_mutex);
        osd_maps_t infos;
        client.osd_info2(infos);
        _alba_levels.clear();
        _osd_maps.clear();
        for (auto &p : infos) {
          _alba_levels.push_back(std::string(p.first));
          _osd_maps.push_back(std::move(p));
        }
      } catch (std::exception &e) {
        ALBA_LOG(INFO,
                 "OSDAccess::update: exception while filling up: " << e.what());
        result = false;
      }
      _filling.store(false);
      _filling_cond.notify_all();
    }
  } else {
    std::unique_lock<std::mutex> lock(_filling_mutex);
    _filling_cond.wait(lock, [this] { return !(this->_filling.load()); });
  }
  return result;
}

std::vector<alba_id_t> OsdAccess::get_alba_levels(Proxy_client &client) {
  if (_alba_levels.size() == 0) {
    if (!this->update(client)) {
      throw osd_access_exception(
          -1, "initial update of osd infos in osd_access failed");
    }
  }
  return _alba_levels;
}

int OsdAccess::read_osds_slices(
    std::map<osd_t, std::vector<asd_slice>> &per_osd) {

  int rc = 0;
  for (auto &item : per_osd) {
    osd_t osd = item.first;
    auto &osd_slices = item.second;
    // TODO this could be done in parallel
    rc = _read_osd_slices_asd_direct_path(osd, osd_slices);
    if (rc) {
      break;
    }
  }
  return rc;
}

int OsdAccess::_read_osd_slices_asd_direct_path(
    osd_t osd, std::vector<asd_slice> &slices) {
  auto maybe_ic = _find_osd(osd);
  if (nullptr == maybe_ic) {
    ALBA_LOG(WARNING, "have context, but no info?");
    return -1;
  }
  auto p = asd_connection_pools.get_connection_pool(
      maybe_ic->first, _connection_pool_size, _timeout);
  if (nullptr == p) {
    return -1;
  }
  auto connection = p->get_connection();

  if (connection) {
    try {
      // TODO 1 batch call...
      for (auto &slice_ : slices) {
        alba::asd_protocol::slice slice__;
        slice__.offset = slice_.offset;
        slice__.length = slice_.len;
        slice__.target = slice_.target;
        std::vector<alba::asd_protocol::slice> slices_{slice__};
        connection->partial_get(slice_.key, slices_);
      }
      p->release_connection(std::move(connection));
      return 0;
    } catch (std::exception &e) {
      p->report_failure();
      ALBA_LOG(INFO, "exception in _read_osd_slices_asd_direct_path for osd "
                         << osd << " " << e.what());
      return -1;
    }
  } else {
    // asd was disqualified
    return -2;
  }
}

std::ostream &operator<<(std::ostream &os, const asd_slice &s) {
  os << "asd_slice{ _"
     << ", " << s.offset << ", " << s.len << ", _"
     << "}";
  return os;
}
}
}
