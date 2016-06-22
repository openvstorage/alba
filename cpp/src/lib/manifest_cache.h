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
#include <utility>
#include <string>
#include <map>
#include <mutex>
#include "manifest.h"

namespace alba {
namespace proxy_client {

typedef std::pair<std::string, std::string> strpair;

using namespace proxy_protocol;
typedef std::map<strpair, std::unique_ptr<Manifest>> manifest_cache;
class ManifestCache {
public:
  static ManifestCache &getInstance();

  ManifestCache(ManifestCache const &) = delete;
  void operator=(ManifestCache const &) = delete;

  void add(strpair key, std::unique_ptr<Manifest> mfp);

  manifest_cache::iterator find(strpair &key);

  manifest_cache::iterator end();

private:
  ManifestCache() {}

  std::mutex _cache_mutex;
  std::map<strpair, std::unique_ptr<Manifest>> _cache;
};
}
}
