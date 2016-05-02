/*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "proxy_protocol.h"
#include <string.h>

#define _LIST_NAMESPACES 1
#define _NAMESPACE_EXISTS 2
#define _CREATE_NAMESPACE 3
#define _DELETE_NAMESPACE 4
#define _LIST_OBJECTS 5
#define _DELETE_OBJECT 8
#define _GET_OBJECT_INFO 9
#define _READ_OBJECT_FS 10
#define _WRITE_OBJECT_FS 11
//#define _READ_OBJECTS_SLICES 12 // deprecated.
#define _READ_OBJECTS_SLICES 13
#define _INVALIDATE_CACHE 14
#define _DROP_CACHE 16
#define _GET_PROXY_VERSION 17
#define _PING 20

namespace alba {
namespace proxy_protocol {

using llio::to;
using llio::from;

void write_tag(message_builder &mb, uint32_t tag) { to<uint32_t>(mb, tag); }

void read_status(message &m, Status &status) {
  uint32_t rc;
  from(m, rc);

  status.set_rc(rc);
  if (rc != 0) {
    string what;
    from(m, what);
    status._what = what;
  }
}

void write_range_params(message_builder &mb, const string &first,
                        const bool finc, const optional<string> &last,
                        const bool linc, const int max, const bool reverse) {
  to(mb, first);
  to(mb, finc);

  boost::optional<std::pair<string, bool>> lasto;
  if (boost::none == last) {
    lasto = boost::none;
  } else {
    lasto = std::pair<string, bool>(*last, linc);
  }
  to(mb, lasto);

  to<uint32_t>(mb, max);
  to(mb, reverse);
}

void write_list_namespaces_request(message_builder &mb, const string &first,
                                   const bool finc,
                                   const optional<string> &last,
                                   const bool linc, const int max,
                                   const bool reverse) {
  write_tag(mb, _LIST_NAMESPACES);
  write_range_params(mb, first, finc, last, linc, max, reverse);
}
void read_list_namespaces_response(message &m, Status &status,
                                   std::vector<string> &namespaces,
                                   bool &has_more) {
  read_status(m, status);
  if (status.is_ok()) {
    from(m, namespaces);
    from(m, has_more);
  }
}

void write_namespace_exists_request(message_builder &mb, const string &name) {
  write_tag(mb, _NAMESPACE_EXISTS);
  to(mb, name);
}
void read_namespace_exists_response(message &m, Status &status, bool &exists) {
  read_status(m, status);
  if (status.is_ok()) {
    from(m, exists);
  }
}

void write_create_namespace_request(message_builder &mb, const string &name,
                                    const optional<string> &preset_name) {
  write_tag(mb, _CREATE_NAMESPACE);
  to(mb, name);
  to(mb, preset_name);
}
void read_create_namespace_response(message &m, Status &status) {
  read_status(m, status);
}

void write_delete_namespace_request(message_builder &mb, const string &name) {
  write_tag(mb, _DELETE_NAMESPACE);
  to(mb, name);
}
void read_delete_namespace_response(message &m, Status &status) {
  read_status(m, status);
}

void write_list_objects_request(message_builder &mb, const string &namespace_,
                                const string &first, const bool finc,
                                const optional<string> &last, const bool linc,
                                const int max, const bool reverse) {
  write_tag(mb, _LIST_OBJECTS);
  to(mb, namespace_);
  write_range_params(mb, first, finc, last, linc, max, reverse);
}

void read_list_objects_response(message &m, Status &status,
                                std::vector<string> &objects, bool &has_more) {
  read_status(m, status);
  if (status.is_ok()) {
    from(m, objects);
    from(m, has_more);
  }
}

void write_read_object_fs_request(message_builder &mb, const string &namespace_,
                                  const string &object_name,
                                  const string &dest_file,
                                  const bool consistent_read,
                                  const bool should_cache) {
  write_tag(mb, _READ_OBJECT_FS);
  to(mb, namespace_);
  to(mb, object_name);
  to(mb, dest_file);
  to(mb, consistent_read);
  to(mb, should_cache);
}

void read_read_object_fs_response(message &m, Status &status) {
  read_status(m, status);
}

void write_write_object_fs_request(message_builder &mb,
                                   const string &namespace_,
                                   const string &object_name,
                                   const string &input_file,
                                   const bool allow_overwrite,
                                   const Checksum *checksum) {
  write_tag(mb, _WRITE_OBJECT_FS);
  to(mb, namespace_);
  to(mb, object_name);
  to(mb, input_file);
  to(mb, allow_overwrite);
  if (nullptr == checksum) {
    to<boost::optional<const Checksum *>>(mb, boost::none);
  } else {
    to(mb, boost::optional<const Checksum *>(checksum));
  }
}

void read_write_object_fs_response(message &m, Status &status) {
  read_status(m, status);
}

void write_delete_object_request(message_builder &mb, const string &namespace_,
                                 const string &object_name,
                                 const bool may_not_exist) {
  write_tag(mb, _DELETE_OBJECT);
  to(mb, namespace_);
  to(mb, object_name);
  to(mb, may_not_exist);
}

void read_delete_object_response(message &m, Status &status) {
  read_status(m, status);
}

void write_get_object_info_request(message_builder &mb,
                                   const string &namespace_,
                                   const string &object_name,
                                   const bool consistent_read,
                                   const bool should_cache) {
  write_tag(mb, _GET_OBJECT_INFO);
  to(mb, namespace_);
  to(mb, object_name);
  to(mb, consistent_read);
  to(mb, should_cache);
}

void read_get_object_info_response(message &m, Status &status, uint64_t &size,
                                   Checksum *&checksum) {
  read_status(m, status);
  if (status.is_ok()) {
    from(m, size);
    from(m, checksum);
  }
}

void write_read_objects_slices_request(message_builder &mb,
                                       const string &namespace_,
                                       const std::vector<ObjectSlices> &slices,
                                       const bool consistent_read) {
  write_tag(mb, _READ_OBJECTS_SLICES);
  to(mb, namespace_);
  to(mb, slices);
  to(mb, consistent_read);
}

void read_read_objects_slices_response(
    message &m, Status &status,
    const std::vector<ObjectSlices> &objects_slices) {
  read_status(m, status);
  if (status.is_ok()) {
    uint32_t size;
    from<uint32_t>(m, size);

    for (auto &object_slices : objects_slices) {
      for (auto &slice : object_slices.slices) {
        memcpy(slice.buf, m.current(), slice.size);
        m.skip(slice.size);
      }
    }
  }
}

void write_invalidate_cache_request(message_builder &mb,
                                    const string &namespace_) {
  write_tag(mb, _INVALIDATE_CACHE);
  to(mb, namespace_);
}

void read_invalidate_cache_response(message &m, Status &status) {
  read_status(m, status);
}

void write_drop_cache_request(message_builder &mb, const string &namespace_) {
  write_tag(mb, _DROP_CACHE);
  to(mb, namespace_);
}

void read_drop_cache_response(message &m, Status &status) {
  read_status(m, status);
}

void write_get_proxy_version_request(message_builder &mb) {
  write_tag(mb, _GET_PROXY_VERSION);
}

void read_get_proxy_version_response(message &m, Status &status, int32_t &major,
                                     int32_t &minor, int32_t &patch,
                                     std::string &hash) {
  read_status(m, status);
  if (status.is_ok()) {
    from(m, major);
    from(m, minor);
    from(m, patch);
    from(m, hash);
  }
}

void write_ping_request(message_builder&mb, const double delay){
  write_tag(mb,_PING);
  to(mb, delay);
}

void read_ping_response(message&m, Status &status, double & timestamp){
  read_status(m,status);
  if(status.is_ok()){
    from(m, timestamp);
  }
  
}
}

namespace llio {
template <>
void to(message_builder &mb,
        const proxy_protocol::SliceDescriptor &desc) noexcept {
  to(mb, desc.offset);
  to(mb, desc.size);
}
template <>
void to(message_builder &mb,
        const proxy_protocol::ObjectSlices &slices) noexcept {
  to(mb, slices.object_name);
  to(mb, slices.slices);
}
}
}
