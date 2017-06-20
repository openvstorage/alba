/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/


#pragma once

#include <boost/bimap.hpp>
#include <boost/bimap/list_of.hpp>
#include <boost/bimap/set_of.hpp>
#include <boost/bimap/unordered_set_of.hpp>
#include <boost/optional.hpp>
#include <mutex>
/*
nicked from volumedriver's LRUCacheToo, but

- changed namespace
- removed VERIFY & logging
- removed name attribute
- removed include sandwich and replaced with #pragma once
- changed the name
- added a safe version
*/
namespace ovs {

// This LRU cache contains objects whereas the other one (LRUCache.h) references
// to objects (e.g. files on disk) which require special steps on eviction
// (remove the file).
// Better naming suggestions / ideas for unification welcome.
template <typename K, typename V,
          template <typename...> class Set = boost::bimaps::unordered_set_of>
class UnsafeLRUCache {
public:
  UnsafeLRUCache( // const std::string& name,
      size_t capacity)
      : capacity_(capacity) {
    if (capacity_ == 0) {
      throw std::logic_error("UnsafeLRUCache capacity should be > 0");
    }
  }

  ~UnsafeLRUCache() = default;

  UnsafeLRUCache(const UnsafeLRUCache &) = delete;

  UnsafeLRUCache &operator=(const UnsafeLRUCache &) = delete;

  boost::optional<V> find(const K &k) {
    auto it = bimap_.left.find(k);
    if (it != bimap_.left.end()) {
      bimap_.right.relocate(bimap_.right.end(), bimap_.project_right(it));
      return it->second;
    } else {
      return boost::none;
    }
  }

  void insert(const K &k, const V &v) {
    if (bimap_.size() >= capacity_) {
      bimap_.right.erase(bimap_.right.begin());
    }

    auto r(bimap_.insert(typename Bimap::value_type(k, v)));
    if (not r.second) {
      bimap_.left.erase(k);
      r = bimap_.insert(typename Bimap::value_type(k, v));
    }
  }

  bool erase(const K &k) { return bimap_.left.erase(k); }

  std::vector<K> keys() const {
    std::vector<K> keys;
    keys.reserve(bimap_.size());

    for (const auto &v : bimap_.right) {
      keys.push_back(v.second);
    }

    return keys;
  }

  void clear() { bimap_.clear(); }

  bool empty() const { return bimap_.empty(); }

  size_t size() const { return bimap_.size(); }

  size_t capacity() const { return capacity_; }

private:
  using Bimap = boost::bimaps::bimap<Set<K>, boost::bimaps::list_of<V>>;
  Bimap bimap_;
  const size_t capacity_;
};

template <typename K, typename V,
          template <typename...> class Set = boost::bimaps::unordered_set_of>

class SafeLRUCache {
public:
  SafeLRUCache(size_t capacity) : _cache(capacity) {}
  void insert(const K &k, const V &v) {
    std::lock_guard<std::mutex> lock(_mutex);
    _cache.insert(k, v);
  }

  boost::optional<V> find(const K &k) {
    std::lock_guard<std::mutex> lock(_mutex);
    return _cache.find(k);
  }

private:
  UnsafeLRUCache<K, V> _cache;
  std::mutex _mutex;
};
}
