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
#include "manifest_cache.h"
#include "osd_access.h"
#include "partial_read_planner.h"

namespace alba {
namespace proxy_client {
using std::string;

RoraProxy_client::RoraProxy_client(
    std::unique_ptr<GenericProxy_client> delegate,
    const RoraConfig &rora_config)
    : _delegate(std::move(delegate)) {
  ALBA_LOG(INFO, "RoraProxy_client(...)");
  ManifestCache::set_capacity(rora_config.manifest_cache_size);
}

bool RoraProxy_client::namespace_exists(const string &name) {
  return _delegate->namespace_exists(name);
};

void RoraProxy_client::create_namespace(
    const string &name, const boost::optional<string> &preset_name) {
  _delegate->create_namespace(name, preset_name);
};

void RoraProxy_client::delete_namespace(const string &name) {
  _delegate->delete_namespace(name);
};

std::tuple<std::vector<string>, has_more> RoraProxy_client::list_namespaces(
    const string &first, const include_first include_first_,
    const boost::optional<string> &last, const include_last include_last_,
    const int max, const reverse reverse_) {
  return _delegate->list_namespaces(first, include_first_, last, include_last_,
                                    max, reverse_);
}

void _maybe_add_to_manifest_cache(const string &namespace_,
                                  const string &object_name,
                                  std::shared_ptr<RoraMap> rora_map) {
  using alba::stuff::operator<<;
  ALBA_LOG(DEBUG, "_maybe_add_to_manifest_cache(..., rora_map = " << rora_map
                                                                  << ")");
  if (compressor_t::NO_COMPRESSION ==
          rora_map->back->compression->get_compressor() &&
      encryption_t::NO_ENCRYPTION ==
          rora_map->back->encrypt_info->get_encryption()) {
    ManifestCache::getInstance().add(namespace_, object_name,
                                     std::move(rora_map));
  }
}
void RoraProxy_client::write_object_fs(const string &namespace_,
                                       const string &object_name,
                                       const string &input_file,
                                       const allow_overwrite overwrite,
                                       const Checksum *checksum) {

  std::unique_ptr<ManifestWithNamespaceId> back(new ManifestWithNamespaceId());
  auto p = std::shared_ptr<RoraMap>(new RoraMap);

  write_object_fs3(namespace_, object_name, input_file, overwrite, checksum,
                   *p);

  _maybe_add_to_manifest_cache(namespace_, object_name, std::move(p));
}

void RoraProxy_client::read_object_fs(const string &namespace_,
                                      const string &object_name,
                                      const string &dest_file,
                                      const consistent_read consistent_read_,
                                      const should_cache should_cache_) {
  _delegate->read_object_fs(namespace_, object_name, dest_file,
                            consistent_read_, should_cache_);
}

void RoraProxy_client::delete_object(const string &namespace_,
                                     const string &object_name,
                                     const may_not_exist may_not_exist_) {
  _delegate->delete_object(namespace_, object_name, may_not_exist_);
}

std::tuple<std::vector<string>, has_more> RoraProxy_client::list_objects(
    const string &namespace_, const string &first,
    const include_first include_first_, const boost::optional<string> &last,
    const include_last include_last_, const int max, const reverse reverse_) {
  return _delegate->list_objects(namespace_, first, include_first_, last,
                                 include_last_, max, reverse_);
}

string fragment_key(const uint32_t namespace_id, const string &object_id,
                    uint32_t version_id, uint32_t chunk_id,
                    uint32_t fragment_id) {
  llio::message_builder mb;
  char instance_content_prefix = 'p';
  mb.add_raw(&instance_content_prefix, 1);
  uint32_t zero = 0;
  to(mb, zero);
  char namespace_char = 'n';
  mb.add_raw(&namespace_char, 1);
  llio::to_be(mb, namespace_id);
  char prefix = 'o';
  mb.add_raw(&prefix, 1);
  to(mb, object_id);
  to(mb, chunk_id);
  to(mb, fragment_id);
  to(mb, version_id);
  string r = mb.as_string();
  return r.substr(4, r.size() - 4);
}

void _dump(std::map<osd_t, std::vector<asd_slice>> &per_osd) {
  for (auto &item : per_osd) {
    osd_t osd = item.first;
    auto &asd_slices = item.second;
    std::cout << osd << ": [";
    for (auto &asd_slice : asd_slices) {

      void *p = asd_slice.bytes;
      std::cout << "( " << asd_slice.offset << ", " << asd_slice.len << ", "
                << p << "),";
    }
    std::cout << "]," << std::endl;
  }
}

void RoraProxy_client::_maybe_update_osd_infos(
    alba_id_t& alba_id,
    std::map<osd_t, std::vector<asd_slice>> &per_osd){

  ALBA_LOG(DEBUG, "RoraProxy_client::_maybe_update_osd_infos(" << alba_id << ",_)");
  bool ok = true;
  for (auto &item : per_osd) {
    osd_t osd = item.first;
    if (OsdAccess::getInstance().osd_is_unknown(alba_id, osd)) {
      ok = false;
      break;
    }
  }

  if (!ok) {
    ALBA_LOG(DEBUG, "RoraProxy_client:: refresh from proxy");
    rora_osd_map_t to_update;
    this->osd_info2(to_update);
    OsdAccess::getInstance().update(to_update);

  }
}

int RoraProxy_client::_short_path_back_one(
    const ObjectSlices &object_slices,
    std::shared_ptr<ManifestWithNamespaceId> mfp,
    std::shared_ptr<alba_id_t> alba_id
    ) {

  // one object, maybe multiple slices and or fragments involved
  std::map<osd_t, std::vector<asd_slice>> per_osd;
  const ManifestWithNamespaceId &manifest_w = *mfp;
  for (auto &sd : object_slices.slices) {
    uint32_t bytes_to_read = sd.size;
    uint32_t offset = sd.offset;
    byte *buf = sd.buf;

    while (bytes_to_read > 0) {
      auto maybe_coords = manifest_w.to_chunk_fragment(offset);
      if (maybe_coords == boost::none) {
        return -1;
      }

      auto &coords = *maybe_coords;

      osd_t osd = coords._osd;
      auto it = per_osd.find(osd);
      if (it == per_osd.end()) {
        std::vector<asd_slice> slices;
        per_osd.insert(make_pair(osd, slices));
        it = per_osd.find(osd);
      }

      uint32_t bytes_in_slice;
      if (coords.pos_in_fragment + bytes_to_read <= coords.fragment_length) {
        bytes_in_slice = bytes_to_read;
      } else {
        bytes_in_slice = coords.fragment_length - coords.pos_in_fragment;
      }

      string key = fragment_key(manifest_w.namespace_id, manifest_w.object_id,
                                coords.fragment_version, coords.chunk_index,
                                coords.fragment_index);

      auto slice = asd_slice{key, coords.pos_in_fragment, bytes_in_slice, buf};

      it->second.push_back(slice);
      buf += bytes_in_slice;
      offset += bytes_in_slice;
      bytes_to_read -= bytes_in_slice;
    }
  }
  // everything to read is now nicely sorted per osd.
  _maybe_update_osd_infos(*alba_id, per_osd);
  //_dump(per_osd);
  return OsdAccess::getInstance().read_osds_slices(*alba_id, per_osd);
}

int RoraProxy_client::_short_path_back_many(
    const std::vector<short_path_back_entry> &short_path,
    std::shared_ptr<alba_id_t> &alba_id
    ) {

  ALBA_LOG(DEBUG, "_short_path_back_many(n_slices =" << short_path.size()
                                                     << ")");

  int result = 0;
  for (auto &object_slices_mf : short_path) {
    auto object_slices = object_slices_mf.first;
    // in // ?
    int current_result =
        _short_path_back_one(object_slices, object_slices_mf.second, alba_id);
    ALBA_LOG(DEBUG, "current_result=" << current_result);
    result |= current_result;
  }
  return result;
}

int RoraProxy_client::_short_path_front_many(
    const std::vector<short_path_front_entry> &short_path) {
  ALBA_LOG(DEBUG, "_short_path_front_many( ... n_slices = " << short_path.size()
                                                            << ")");
  for (auto &entry : short_path) {
    auto object_slices = entry.first;
    const std::shared_ptr<RoraMap> rora_map = entry.second;
    ALBA_LOG(DEBUG, "object_name = " << object_slices.object_name);

    for (auto &slice : object_slices.slices) {
      // which manifest for the start point of the slice ?
      auto r = proxy_protocol::lookup(*rora_map, slice.offset);
      if (boost::none != r) {
        ALBA_LOG(DEBUG, "HIERE:" << *r);

      } else {
        throw "not implemented...";
      }
    }
  }
  throw "not implemented";
}

void _process(std::vector<object_info> &object_infos,
              const string &namespace_) {

  ALBA_LOG(DEBUG, "_process : " << object_infos.size());
  for (auto &object_info : object_infos) {
    using alba::stuff::operator<<;
    // ALBA_LOG(DEBUG, "_procesing object_info:" << object_info);

    const string &object_name = std::get<0>(object_info);
    auto &future = std::get<1>(object_info);

    ALBA_LOG(WARNING, "_process front?");

    if (future == "") {
      std::shared_ptr<RoraMap> rora_map(new RoraMap);
      rora_map->back = std::move(std::get<2>(object_info));
      _maybe_add_to_manifest_cache(namespace_, object_name, rora_map);
    }
  }
}

void RoraProxy_client::read_objects_slices(
    const string &namespace_, const std::vector<ObjectSlices> &slices,
    const consistent_read consistent_read_) {

  if (consistent_read_ == consistent_read::T) {
    std::vector<object_info> object_infos;
    _delegate->read_objects_slices2(namespace_, slices, consistent_read_,
                                    object_infos);
    _process(object_infos, namespace_);
  } else {
    std::vector<short_path_front_entry> short_path_front;
    std::vector<short_path_back_entry> short_path_back;
    std::vector<ObjectSlices> via_proxy;
    for (auto &object_slices : slices) {
      auto object_name = object_slices.object_name;
      auto &cache = ManifestCache::getInstance();
      auto entry = cache.find(namespace_, object_name);
      if (nullptr != entry) {
        auto front = entry->front;
        if (nullptr != front) {
          auto p = std::make_pair(object_slices, entry);
          short_path_front.push_back(p);
        } else {
          auto mfp = entry->back;
          if (compressor_t::NO_COMPRESSION ==
                  mfp->compression->get_compressor() &&
              encryption_t::NO_ENCRYPTION ==
                  mfp->encrypt_info->get_encryption()) {
            auto p = std::make_pair(object_slices, mfp);
            short_path_back.push_back(p);
          } else {
            via_proxy.push_back(object_slices);
          }
        }
      } else {
        via_proxy.push_back(object_slices);
      }
    };
    // TODO: different paths could go in parallel
    int result_front = _short_path_front_many(short_path_front);

    std::vector<object_info> object_infos;

    _delegate->read_objects_slices2(namespace_, via_proxy, consistent_read_,
                                    object_infos);

    _process(object_infos, namespace_);

    // short_path_back.
    std::shared_ptr<alba_id_t> back_id(nullptr);
    int result = _short_path_back_many(short_path_back, back_id);
    ALBA_LOG(DEBUG, "_short_path_many => " << result);
    if (result) {
      ALBA_LOG(DEBUG, "result=" << result << " => partial read via delegate");
      std::vector<ObjectSlices> via_proxy2;
      for (auto &p : short_path_back) {
        auto object_slices = p.first;
        via_proxy2.push_back(object_slices);
      }
      std::vector<object_info> object_infos2;
      _delegate->read_objects_slices2(namespace_, via_proxy2, consistent_read_,
                                      object_infos2);
      _process(object_infos2, namespace_);
    }
  }
}

void RoraProxy_client::write_object_fs2(const string &namespace_,
                                        const string &object_name,
                                        const string &input_file,
                                        const allow_overwrite allow_overwrite_,
                                        const Checksum *checksum,
                                        ManifestWithNamespaceId &mf) {
  return _delegate->write_object_fs2(namespace_, object_name, input_file,
                                     allow_overwrite_, checksum, mf);
}

void RoraProxy_client::write_object_fs3(const string &namespace_,
                                        const string &object_name,
                                        const string &input_file,
                                        const allow_overwrite allow_overwrite_,
                                        const Checksum *checksum,
                                        RoraMap &rora_map) {
  ALBA_LOG(DEBUG, "write_object_fs3(" << namespace_ << ", " << object_name
                                      << "...)");
  return _delegate->write_object_fs3(namespace_, object_name, input_file,
                                     allow_overwrite_, checksum, rora_map);
}

std::tuple<uint64_t, Checksum *> RoraProxy_client::get_object_info(
    const string &namespace_, const string &object_name,
    const consistent_read consistent_read_, const should_cache should_cache_) {
  return _delegate->get_object_info(namespace_, object_name, consistent_read_,
                                    should_cache_);
}

void RoraProxy_client::invalidate_cache(const string &namespace_) {
  ManifestCache::getInstance().invalidate_namespace(namespace_);
  _delegate->invalidate_cache(namespace_);
}

void RoraProxy_client::drop_cache(const string &namespace_) {
  _delegate->drop_cache(namespace_);
}

std::tuple<int32_t, int32_t, int32_t, string>
RoraProxy_client::get_proxy_version() {
  return _delegate->get_proxy_version();
}

double RoraProxy_client::ping(const double delay) {
  return _delegate->ping(delay);
}

void RoraProxy_client::osd_info(osd_map_t &result) {
  ALBA_LOG(DEBUG, "RoraProxy_client::osd_info");
  _delegate->osd_info(result);
}

void RoraProxy_client::osd_info2(rora_osd_map_t &result) {
  ALBA_LOG(DEBUG, "RoraProxy_client::osd_info2");
  _delegate->osd_info2(result);
}
}
}
