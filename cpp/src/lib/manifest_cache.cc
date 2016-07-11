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

void ManifestCache::add(strpair key, std::shared_ptr<Manifest> mfp) {
  ALBA_LOG(DEBUG, "ManifestCache::add");

  std::lock_guard<std::mutex> lock(_cache_mutex);
  auto it = _cache.find(key);
  if (it != _cache.end()) {
    _cache.erase(it);
  }

  if (_cache.size() > _manifest_cache_capacity) {
    it = _cache.begin();
    const strpair &key = it->first;
    using namespace alba::stuff;
    ALBA_LOG(DEBUG, "cache is full(" << _manifest_cache_capacity
                                     << "), evicting victim: " << key);

    _cache.erase(it);
  }

  _cache[key] = std::move(mfp);
}

std::shared_ptr<Manifest> ManifestCache::find(strpair &key) {
  std::lock_guard<std::mutex> g(_cache_mutex);
  auto it = _cache.find(key);
  if (it == _cache.end()) {
    return nullptr;
  } else {
    return it->second;
  }
}

void ManifestCache::invalidate_namespace(const std::string &namespace_) {
  ALBA_LOG(DEBUG, "ManifestCache::invalidate_namespace(" << namespace_ << ")");
  std::lock_guard<std::mutex> g(_cache_mutex);
  _cache.clear();
}
}
}
