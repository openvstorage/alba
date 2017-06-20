/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include "proxy_client.h"

namespace alba {
namespace proxy_client {

using namespace proxy_protocol;
class GenericProxy_client : public Proxy_client {
public:
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

  virtual void
  read_objects_slices(const std::string &namespace_,
                      const std::vector<proxy_protocol::ObjectSlices> &,
                      const consistent_read, alba::statistics::RoraCounter &);
  virtual void
  read_objects_slices2(const std::string &namespace_,
                       const std::vector<proxy_protocol::ObjectSlices> &,
                       const consistent_read,
                       std::vector<proxy_protocol::object_info> &,
                       alba::statistics::RoraCounter &);

  virtual void write_object_fs2(const std::string &namespace_,
                                const std::string &object_name,
                                const std::string &input_file,
                                const allow_overwrite, const Checksum *checksum,
                                proxy_protocol::ManifestWithNamespaceId &);

  virtual void update_session(
      const std::vector<std::pair<std::string, boost::optional<std::string>>>
          &args,
      std::vector<std::pair<std::string, std::string>> &processed_kvs);
  virtual std::tuple<uint64_t, Checksum *>
  get_object_info(const std::string &namespace_, const std::string &object_name,
                  const consistent_read, const should_cache);

  virtual void
  apply_sequence(const std::string &namespace_, const write_barrier,
                 const std::vector<std::shared_ptr<sequences::Assert>> &,
                 const std::vector<std::shared_ptr<sequences::Update>> &);

  // TODO want to make this a protected member but then rora_proxy_client can't
  // use it
  virtual void
  apply_sequence_(const std::string &namespace_, const write_barrier,
                  const std::vector<std::shared_ptr<sequences::Assert>> &,
                  const std::vector<std::shared_ptr<sequences::Update>> &,
                  std::vector<proxy_protocol::object_info> &);

  virtual void invalidate_cache(const std::string &namespace_);

  virtual void drop_cache(const std::string &namespace_);

  virtual std::tuple<int32_t, int32_t, int32_t, std::string>
  get_proxy_version();

  virtual double ping(const double delay);

  virtual void osd_info(osd_map_t &result);

  virtual void osd_info2(osd_maps_t &result);

  virtual bool has_local_fragment_cache();

  virtual boost::optional<string>
  get_fragment_encryption_key(const string &alba_id,
                              const namespace_t namespace_id);

  GenericProxy_client(const std::chrono::steady_clock::duration &timeout,
                      std::unique_ptr<transport::Transport> &&);

  virtual ~GenericProxy_client(){};

protected:
  void init_();

  // these methods delegate to _transport
  void _expires_from_now(const std::chrono::steady_clock::duration &timeout);
  void _output();
  llio::message _input();

  proxy_protocol::Status _status;
  std::unique_ptr<transport::Transport> _transport;
  const std::chrono::steady_clock::duration _timeout;
  message_builder _mb;

  void check_status(const char *function_name);
};
}
}
