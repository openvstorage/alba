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
#include "lru_cache.h"
#include "manifest.h"
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
namespace alba {
namespace proxy_client {

using namespace proxy_protocol;
typedef std::shared_ptr<ManifestWithNamespaceId> manifest_cache_entry;
typedef ovs::LRUCacheToo<std::string, manifest_cache_entry> manifest_cache;
class ManifestCache {
public:
  static ManifestCache &getInstance();
  static void set_capacity(size_t capacity);

  ManifestCache(ManifestCache const &) = delete;
  void operator=(ManifestCache const &) = delete;

  void add(std::string namespace_, std::string alba_id,
           manifest_cache_entry rora_map);

  manifest_cache_entry find(const std::string &namespace_,
                            const std::string &alba_id,
                            const std::string &object_name);

  void invalidate_namespace(const std::string &);

private:
  ManifestCache() {}

  std::mutex _level1_mutex;
  std::map<std::string, std::pair<std::shared_ptr<manifest_cache>,
                                  std::shared_ptr<std::mutex>>>
      _level1;
};
}
}
