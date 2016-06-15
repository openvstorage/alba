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
#include "proxy_client.h"

namespace alba{
namespace proxy_client {
typedef std::pair<std::string, std::string> strpair;

class RoraProxy_client : public Proxy_client{
public:
    RoraProxy_client(std::unique_ptr<Proxy_client> delegate);

    virtual bool
        namespace_exists(const std::string &name);

    virtual void
        create_namespace(const std::string &name,
                         const boost::optional<std::string> &preset_name);

    virtual void
        delete_namespace(const std::string &name);

    virtual std::tuple<std::vector<std::string>, has_more>
        list_namespaces(const std::string &first, const include_first,
                        const boost::optional<std::string> &last, const include_last,
                        const int max, const reverse reverse = reverse::F);

    virtual void write_object_fs(const std::string &namespace_,
                                 const std::string &object_name,
                                 const std::string &input_file,
                                 const allow_overwrite,
                                 const Checksum *checksum);

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
                          const consistent_read);

  virtual void
  write_object_fs2(const std::string &namespace_,
                   const std::string &object_name,
                   const std::string &input_file, const allow_overwrite,
                   const Checksum *checksum,
                   proxy_protocol::Manifest&
      );

  virtual std::tuple<uint64_t, Checksum *>
  get_object_info(const std::string &namespace_, const std::string &object_name,
                  const consistent_read, const should_cache);

  virtual void invalidate_cache(const std::string &namespace_);

  virtual void drop_cache(const std::string &namespace_);

  virtual std::tuple<int32_t, int32_t, int32_t, std::string>
  get_proxy_version();

  virtual double ping(const double delay);
  virtual void osd_info(std::vector<std::pair<osd_t, proxy_protocol::OsdInfo>>& result);
  virtual ~RoraProxy_client(){};

private:
  typedef std::pair<proxy_protocol::ObjectSlices, proxy_protocol::Manifest&>
      short_path_entry;

  std::unique_ptr<Proxy_client> _delegate;
  std::map<strpair, std::unique_ptr<proxy_protocol::Manifest>> _cache;

  bool _short_path_many(const std::string& namespace_,
                        const std::vector<short_path_entry> & short_path);
  bool _short_path_one(const std::string& namespace_,
                       const proxy_protocol::ObjectSlices& object_slices,
                       const proxy_protocol::Manifest& manifest);

};

std::string fragment_key(const std::string& object_id,
                         uint32_t version_id,
                         uint32_t chunk_id,
                         uint32_t fragment_id
    );


}
}
