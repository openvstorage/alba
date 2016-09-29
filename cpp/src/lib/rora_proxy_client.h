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
#include "generic_proxy_client.h"
#include "osd_access.h"
#include "osd_info.h"
#include "proxy_client.h"

namespace alba {
namespace proxy_client {
typedef std::pair<std::string, std::string> strpair;
using namespace proxy_protocol;
typedef std::pair<ObjectSlices, std::shared_ptr<ManifestWithNamespaceId>>
    short_path_back_entry;

typedef std::pair<ObjectSlices, std::shared_ptr<ManifestWithNamespaceId>>
    short_path_front_entry;

class RoraProxy_client : public Proxy_client {
public:
  RoraProxy_client(std::unique_ptr<GenericProxy_client> delegate,
                   const RoraConfig &);

  virtual bool namespace_exists(const std::string &name);

  virtual void
  create_namespace(const std::string &name,
                   const boost::optional<std::string> &preset_name);

  virtual void delete_namespace(const std::string &name);

  virtual std::tuple<std::vector<std::string>, has_more>
  list_namespaces(const std::string &first, const include_first,
                  const boost::optional<std::string> &last, const include_last,
                  const int max, const reverse reverse = reverse::F);

  virtual void write_object_fs(const std::string &namespace_,
                               const std::string &object_name,
                               const std::string &input_file,
                               const allow_overwrite, const Checksum *checksum);

  virtual void read_object_fs(const std::string &namespace_,
                              const std::string &object_name,
                              const std::string &dest_file,
                              const consistent_read, const should_cache);

  virtual void delete_object(const std::string &namespace_,
                             const std::string &object_name,
                             const may_not_exist);

  virtual std::tuple<std::vector<std::string>, has_more>
  list_objects(const std::string &namespace_, const std::string &first,
               const include_first, const boost::optional<std::string> &last,
               const include_last, const int max,
               const reverse reverse = reverse::F);

  virtual void read_objects_slices(const std::string &namespace_,
                                   const std::vector<ObjectSlices> &,
                                   const consistent_read);

  virtual void write_object_fs2(const std::string &namespace_,
                                const std::string &object_name,
                                const std::string &input_file,
                                const allow_overwrite, const Checksum *checksum,
                                ManifestWithNamespaceId &);

  virtual std::tuple<uint64_t, Checksum *>
  get_object_info(const std::string &namespace_, const std::string &object_name,
                  const consistent_read, const should_cache);

  virtual void
  apply_sequence(const std::string &namespace_, const write_barrier,
                 const std::vector<std::shared_ptr<sequences::Assert>> &,
                 const std::vector<std::shared_ptr<sequences::Update>> &,
                 std::vector<proxy_protocol::object_info> &);

  virtual void invalidate_cache(const std::string &namespace_);

  virtual void drop_cache(const std::string &namespace_);

  virtual std::tuple<int32_t, int32_t, int32_t, std::string>
  get_proxy_version();

  virtual double ping(const double delay);
  virtual void osd_info(osd_map_t &);
  virtual void osd_info2(rora_osd_map_t &);

  virtual ~RoraProxy_client(){};

private:
  std::unique_ptr<GenericProxy_client> _delegate;

  void
  _maybe_update_osd_infos(alba_id_t &alba_id,
                          std::map<osd_t, std::vector<asd_slice>> &per_osd);

  int _short_path_front_many(
      const std::vector<short_path_front_entry> &short_path);

  int _short_path_back_many(
      const std::vector<short_path_back_entry> &short_path,
      std::shared_ptr<alba_id_t> &alba_id);

  int _short_path_back_one(
      const proxy_protocol::ObjectSlices &object_slices,
      std::shared_ptr<proxy_protocol::ManifestWithNamespaceId> mfp,
      std::shared_ptr<alba_id_t> alba_id);
};

std::string fragment_key(const std::string &object_id, uint32_t version_id,
                         uint32_t chunk_id, uint32_t fragment_id);
}
}
