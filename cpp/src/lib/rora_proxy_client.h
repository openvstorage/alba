/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include "generic_proxy_client.h"
#include "osd_access.h"
#include "osd_info.h"
#include "proxy_client.h"

#include <unordered_map>

namespace alba {
namespace proxy_client {

using namespace proxy_protocol;
using namespace std::chrono;

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
                                   const consistent_read,
                                   alba::statistics::RoraCounter &);

  virtual std::tuple<uint64_t, Checksum *>
  get_object_info(const std::string &namespace_, const std::string &object_name,
                  const consistent_read, const should_cache);

  virtual void
  apply_sequence(const std::string &namespace_, const write_barrier,
                 const std::vector<std::shared_ptr<sequences::Assert>> &,
                 const std::vector<std::shared_ptr<sequences::Update>> &);

  virtual void invalidate_cache(const std::string &namespace_);

  virtual void drop_cache(const std::string &namespace_);

  virtual std::tuple<int32_t, int32_t, int32_t, std::string>
  get_proxy_version();

  virtual double ping(const double delay);
  virtual void osd_info(osd_map_t &);
  virtual void osd_info2(osd_maps_t &);

  virtual boost::optional<string>
  get_fragment_encryption_key(const string &alba_id,
                              const namespace_t namespace_id);

  virtual ~RoraProxy_client(){};

private:
  std::unique_ptr<GenericProxy_client> _delegate;

  void _process(std::vector<object_info> &object_infos,
                const string &namespace_);

  void
  _maybe_update_osd_infos(std::map<osd_t, std::vector<asd_slice>> &per_osd);

  int _short_path(const std::vector<std::pair<byte *, Location>> &);

  bool _use_null_io;

  bool _has_local_fragment_cache;

  int _fast_path_failures;
  steady_clock::time_point _failure_time;

  int _asd_connection_pool_size;
  std::chrono::steady_clock::duration _asd_partial_read_timeout;

  message_builder _fkb;
  string _fragment_key(const namespace_t namespace_id, const string &object_id,
                       uint32_t version_id, uint32_t chunk_id,
                       uint32_t fragment_id);
  boost::optional<int> _ser_version;

  void _slow_path(const std::string &namespace_,
                  const std::vector<ObjectSlices> &, const consistent_read,
                  std::vector<object_info> &object_infos,
                  alba::statistics::RoraCounter &);

  std::unordered_map<string, string> _enc_keys;
  string get_encryption_key(const string &alba_id,
                            const namespace_t namespace_id,
                            const string &key_identification);
};

std::string fragment_key(const std::string &object_id, uint32_t version_id,
                         uint32_t chunk_id, uint32_t fragment_id);
}
}
