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

#include "manifest_cache.h"

namespace alba {
namespace proxy_client {

ManifestCache &ManifestCache::getInstance() {
  static ManifestCache instance;
  return instance;
}

static size_t _manifest_cache_capacity = 10000;
void ManifestCache::set_capacity(size_t capacity) {
  _manifest_cache_capacity = capacity;
}

void ManifestCache::add(std::string namespace_, std::string object_name,
                        manifest_cache_entry mfp) {
  ALBA_LOG(DEBUG, "ManifestCache::add");

  std::shared_ptr<manifest_cache> mcp = nullptr;
  std::shared_ptr<std::mutex> mp = nullptr;
  {
    std::lock_guard<std::mutex> lock(_level1_mutex);
    auto it1 = _level1.find(namespace_);

    if (it1 == _level1.end()) {
      ALBA_LOG(INFO, "ManifestCache::add namespace:'"
                         << namespace_ << "' : new manifest cache");
      std::shared_ptr<manifest_cache> mc(
          new manifest_cache(_manifest_cache_capacity));
      std::shared_ptr<std::mutex> mm(new std::mutex);
      auto p = std::make_pair(mc, mm);
      _level1[namespace_] = std::move(p);
      it1 = _level1.find(namespace_);
    } else {
      ALBA_LOG(DEBUG, "ManifestCache::add namespace:'"
                          << namespace_ << "' : existing manifest cache");
    }
    const auto &v = it1->second;
    mcp = v.first;
    mp = v.second;
  }

  manifest_cache &manifest_cache = *mcp;
  std::mutex &m = *mp;
  {
    std::lock_guard<std::mutex> lock(m);
    manifest_cache.insert(object_name, std::move(mfp));
  }
}

manifest_cache_entry ManifestCache::find(const std::string &namespace_,
                                         const std::string &object_name) {
  std::pair<std::shared_ptr<manifest_cache>, std::shared_ptr<std::mutex>> vp;
  {
    std::lock_guard<std::mutex> g(_level1_mutex);
    auto it = _level1.find(namespace_);
    if (it == _level1.end()) {
      return nullptr;
    } else {
      vp = it->second;
    }
  }
  auto &map = *vp.first;
  auto &mm = *vp.second;
  {
    std::lock_guard<std::mutex> g(mm);
    const auto &maybe_elem = map.find(object_name);
    if (boost::none == maybe_elem) {
      return nullptr;
    } else {
      return *maybe_elem;
    }
  }
}

void ManifestCache::invalidate_namespace(const std::string &namespace_) {
  ALBA_LOG(DEBUG, "ManifestCache::invalidate_namespace(" << namespace_ << ")");
  std::lock_guard<std::mutex> g(_level1_mutex);
  auto it = _level1.find(namespace_);
  if (it != _level1.end()) {
    _level1.erase(it);
  }
}
}
}
