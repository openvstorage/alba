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

#include "rora_proxy_client.h"
#include "manifest.h"
#include "alba_logger.h"

namespace alba {
namespace proxy_client {
RoraProxy_client::RoraProxy_client(std::unique_ptr<Proxy_client> delegate)
    : _delegate(std::move(delegate)) {}

bool RoraProxy_client::namespace_exists(const std::string &name) {
  return _delegate->namespace_exists(name);
};

void RoraProxy_client::create_namespace(
    const std::string &name, const boost::optional<std::string> &preset_name) {
  _delegate->create_namespace(name, preset_name);
};

void RoraProxy_client::delete_namespace(const std::string &name) {
  _delegate->delete_namespace(name);
};

std::tuple<std::vector<std::string>, has_more>
RoraProxy_client::list_namespaces(const std::string &first,
                                  const include_first include_first_,
                                  const boost::optional<std::string> &last,
                                  const include_last include_last_,
                                  const int max, const reverse reverse_) {
  return _delegate->list_namespaces(first, include_first_, last, include_last_,
                                    max, reverse_);
}

void RoraProxy_client::write_object_fs(const std::string &namespace_,
                                       const std::string &object_name,
                                       const std::string &input_file,
                                       const allow_overwrite overwrite,
                                       const Checksum *checksum) {
  proxy_protocol::Manifest mf = _delegate->write_object_fs2(
      namespace_, object_name, input_file, overwrite, checksum);
  ALBA_LOG(DEBUG, mf);
  auto key = std::pair<std::string, std::string>(namespace_, object_name);
  auto value = mf;
  auto it = _cache.find(key);
  if (it != _cache.end()) {
    _cache.erase(it);
  }
  _cache.emplace(key, value);
}

void RoraProxy_client::read_object_fs(const std::string &namespace_,
                                      const std::string &object_name,
                                      const std::string &dest_file,
                                      const consistent_read consistent_read_,
                                      const should_cache should_cache_) {
  _delegate->read_object_fs(namespace_, object_name, dest_file,
                            consistent_read_, should_cache_);
}

void RoraProxy_client::delete_object(const std::string &namespace_,
                                     const std::string &object_name,
                                     const may_not_exist may_not_exist_) {
  _delegate->delete_object(namespace_, object_name, may_not_exist_);
}

std::tuple<std::vector<std::string>, has_more> RoraProxy_client::list_objects(
    const std::string &namespace_, const std::string &first,
    const include_first include_first_,
    const boost::optional<std::string> &last, const include_last include_last_,
    const int max, const reverse reverse_) {
  return _delegate->list_objects(namespace_, first, include_first_, last,
                                 include_last_, max, reverse_);
}

std::string fragment_key(const std::string &object_id, uint32_t version_id,
                         uint32_t chunk_id, uint32_t fragment_id) {
  llio::message_builder mb;
  char prefix = 'o';
  mb.add_raw(&prefix, 1);
  to(mb, object_id);
  to(mb, chunk_id);
  to(mb, fragment_id);
  to(mb, version_id);
  return mb.as_string();
}

using alba::proxy_protocol::byte;
typedef std::tuple<std::string, uint32_t, uint32_t, byte *> asd_slice;
typedef uint32_t osd;

void _dump(std::map<osd, std::vector<asd_slice>> &per_osd) {
  for (auto &item : per_osd) {
    auto osd = item.first;
    auto asd_slices = item.second;
    std::cout << osd << ": [";
    for (auto &asd_slice : asd_slices) {

      void* p = std::get<3>(asd_slice);
      std::cout << "( " << std::get<1>(asd_slice)
                << ", " << std::get<2>(asd_slice)
                << ", " << p
                << "),";
    }
    std::cout << "]," << std::endl;
  }
}

bool RoraProxy_client::_short_path_one(
    const std::string &namespace_,
    const proxy_protocol::ObjectSlices &object_slices,
    const proxy_protocol::Manifest &manifest) {

  // one object, maybe multiple slices and or fragments involved
  ALBA_LOG(DEBUG, "_short_path_one(" << namespace_ << ", ...)");
  std::map<osd, std::vector<asd_slice>> per_osd;

  for (auto &sd : object_slices.slices) {
    uint32_t bytes_to_read = sd.size;
    uint32_t offset = sd.offset;
    byte *buf = sd.buf;

    while (bytes_to_read > 0) {
      auto maybe_coords = manifest.to_chunk_fragment(offset);
      if (maybe_coords == boost::none) {
        return false;
      }


      auto &coords = *maybe_coords;

      uint32_t osd = coords.osd;
      auto it = per_osd.find(osd);
      if (it == per_osd.end()) {
        std::vector<asd_slice> slices;
        per_osd.insert(make_pair(osd,slices));
        it = per_osd.find(osd);
      }

      int bytes_in_slice;
      if (coords.pos_in_fragment + bytes_to_read <= coords.fragment_length) {
        bytes_in_slice = bytes_to_read;
      } else {
        bytes_in_slice = coords.fragment_length - coords.pos_in_fragment;
      }

      std::string key =
          fragment_key(manifest.object_id, coords.fragment_version,
                       coords.chunk_index, coords.fragment_index);

      auto slice = asd_slice(key, coords.pos_in_fragment, bytes_in_slice, buf);

      it->second.push_back(slice);
      buf += bytes_in_slice;
      offset += bytes_in_slice;
      bytes_to_read -= bytes_in_slice;
    }
  }
  // everything to read is now nicely sorted per osd.
  // TODO: contact OSD and fill buffers.
  _dump(per_osd);

  return true;
}

bool RoraProxy_client::_short_path_many(
    const std::string &namespace_,
    const std::vector<std::pair<proxy_protocol::ObjectSlices,
                                proxy_protocol::Manifest>> &short_path) {
  // for now, we can't do it.
  ALBA_LOG(DEBUG, "_short_path_many(" << namespace_ << ", ...)");
  bool result = true;
  for (auto &object_slices_mf : short_path) {
    auto object_slices = object_slices_mf.first;
    auto mf = object_slices_mf.second;
    // in // ?
    result &= _short_path_one(namespace_, object_slices, mf);
  }
  return result;
}

typedef std::pair<proxy_protocol::ObjectSlices, proxy_protocol::Manifest>
    short_path_entry;

void RoraProxy_client::read_objects_slices(
    const std::string &namespace_,
    const std::vector<proxy_protocol::ObjectSlices> &slices,
    const consistent_read consistent_read_) {

  if (consistent_read_ == consistent_read::T) {
    _delegate->read_objects_slices(namespace_, slices, consistent_read_);
  } else {
    std::vector<proxy_protocol::ObjectSlices> via_proxy;
    std::vector<short_path_entry> short_path;
    for (auto &object_slices : slices) {
      auto object_name = object_slices.object_name;
      auto key = strpair(namespace_, object_name);

      auto it = _cache.find(key);
      if (it == _cache.end()) {
        via_proxy.push_back(object_slices);
      } else {
        auto mf = it->second;
        auto p =
            std::pair<proxy_protocol::ObjectSlices, proxy_protocol::Manifest>(
                object_slices, mf);
        short_path.push_back(p);
      }
    };
    // short_path & via_proxy could go in //

    _delegate->read_objects_slices(namespace_, via_proxy, consistent_read_);
    // short_path.
    if (!_short_path_many(namespace_, short_path)) {
      std::vector<proxy_protocol::ObjectSlices> via_proxy2;
      for (auto &p : short_path) {
        auto object_slices = p.first;
        via_proxy2.push_back(object_slices);
      }
      _delegate->read_objects_slices(namespace_, via_proxy2, consistent_read_);
    }
  }
}

proxy_protocol::Manifest RoraProxy_client::write_object_fs2(
    const std::string &namespace_, const std::string &object_name,
    const std::string &input_file, const allow_overwrite allow_overwrite_,
    const Checksum *checksum) {
  return _delegate->write_object_fs2(namespace_, object_name, input_file,
                                     allow_overwrite_, checksum);
}

std::tuple<uint64_t, Checksum *> RoraProxy_client::get_object_info(
    const std::string &namespace_, const std::string &object_name,
    const consistent_read consistent_read_, const should_cache should_cache_) {
  return _delegate->get_object_info(namespace_, object_name, consistent_read_,
                                    should_cache_);
}

void RoraProxy_client::invalidate_cache(const std::string &namespace_) {
  _delegate->invalidate_cache(namespace_);
}

void RoraProxy_client::drop_cache(const std::string &namespace_) {
  _delegate->drop_cache(namespace_);
}

std::tuple<int32_t, int32_t, int32_t, std::string>
RoraProxy_client::get_proxy_version() {
  return _delegate->get_proxy_version();
}

double RoraProxy_client::ping(const double delay) {
  return _delegate->ping(delay);
}
}
}
