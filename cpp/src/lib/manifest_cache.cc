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

void ManifestCache::add(strpair key, std::unique_ptr<Manifest> mfp) {
  ALBA_LOG(DEBUG, "ManifestCache::add");
  auto it = _cache.find(key);
  if (it != _cache.end()) {
    _cache.erase(it);
  }
  _cache.emplace(std::make_pair(key, std::move(mfp)));
}

manifest_cache::iterator ManifestCache::find(strpair &key) {
  return _cache.find(key);
}

manifest_cache::iterator ManifestCache::end() { return _cache.end(); }
}
}
