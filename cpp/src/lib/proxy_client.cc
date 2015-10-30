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

#include "proxy_client.h"
#include "alba_logger.h"

namespace alba {
namespace proxy_client {

using std::string;
using std::vector;
using std::tuple;
using boost::optional;
using llio::message;
using llio::message_builder;

Proxy_client::Proxy_client(
    const string &ip, const string &port,
    const boost::asio::time_traits<boost::posix_time::ptime>::duration_type &
        expiry_time)
    : _status(), _expiry_time(expiry_time) {
  _stream.expires_from_now(_expiry_time);
  _stream.connect(ip, port);
  int32_t magic{1148837403};
  int32_t version{1};
  _stream.write((const char *)(&magic), 4);
  _stream.write((const char *)(&version), 4);
  _stream.expires_at(boost::posix_time::max_date_time);
}

void Proxy_client::check_status(const char *function_name) {
  _stream.expires_at(boost::posix_time::max_date_time);
  if (not _status.is_ok()) {
    ALBA_LOG(DEBUG, function_name
                        << " received rc:" << (uint32_t)_status._return_code)
    throw proxy_exception(_status._return_code, _status._what);
  }
}

tuple<vector<string>, has_more> Proxy_client::list_namespaces(
    const string &first, const include_first finc, const optional<string> &last,
    const include_last linc, const int max, const reverse reverse) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_list_namespaces_request(
      mb, first, BooleanEnumTrue(finc), last, BooleanEnumTrue(linc), max,
      BooleanEnumTrue(reverse));
  mb.output(_stream);

  message response(_stream);
  std::vector<string> namespaces;
  bool has_more_;
  proxy_protocol::read_list_namespaces_response(response, _status, namespaces,
                                                has_more_);

  check_status(__PRETTY_FUNCTION__);

  return tuple<vector<string>, has_more>(namespaces, has_more(has_more_));
}

bool Proxy_client::namespace_exists(const string &name) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_namespace_exists_request(mb, name);
  mb.output(_stream);

  message response(_stream);
  bool exists;
  proxy_protocol::read_namespace_exists_response(response, _status, exists);

  check_status(__PRETTY_FUNCTION__);

  return exists;
}

void
Proxy_client::create_namespace(const string &name,
                               const boost::optional<string> &preset_name) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_create_namespace_request(mb, name, preset_name);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_create_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void Proxy_client::delete_namespace(const string &name) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_delete_namespace_request(mb, name);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_delete_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

tuple<vector<string>, has_more> Proxy_client::list_objects(
    const string &namespace_, const string &first, const include_first finc,
    const optional<string> &last, const include_last linc, const int max,
    const reverse reverse) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_list_objects_request(
      mb, namespace_, first, BooleanEnumTrue(finc), last, BooleanEnumTrue(linc),
      max, BooleanEnumTrue(reverse));
  mb.output(_stream);

  message response(_stream);
  std::vector<string> objects;
  bool has_more_;
  proxy_protocol::read_list_objects_response(response, _status, objects,
                                             has_more_);

  check_status(__PRETTY_FUNCTION__);
  return tuple<vector<string>, has_more>(objects, has_more(has_more_));
}

void Proxy_client::read_object_fs(const string &namespace_,
                                  const string &object_name,
                                  const string &dest_file,
                                  const consistent_read consistent_read,
                                  const should_cache should_cache) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_read_object_fs_request(
      mb, namespace_, object_name, dest_file, BooleanEnumTrue(consistent_read),
      BooleanEnumTrue(should_cache));
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_read_object_fs_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void Proxy_client::write_object_fs(const string &namespace_,
                                   const string &object_name,
                                   const string &input_file,
                                   const allow_overwrite allow_overwrite,
                                   const Checksum *checksum) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_write_object_fs_request(
      mb, namespace_, object_name, input_file, BooleanEnumTrue(allow_overwrite),
      checksum);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_write_object_fs_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void Proxy_client::delete_object(const string &namespace_,
                                 const string &object_name,
                                 const may_not_exist may_not_exist) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_delete_object_request(mb, namespace_, object_name,
                                              BooleanEnumTrue(may_not_exist));
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_delete_object_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

tuple<uint64_t, Checksum *> Proxy_client::get_object_info(
    const string &namespace_, const string &object_name,
    const consistent_read consistent_read, const should_cache should_cache) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_get_object_info_request(
      mb, namespace_, object_name, BooleanEnumTrue(consistent_read),
      BooleanEnumTrue(should_cache));
  mb.output(_stream);

  message response(_stream);
  uint64_t size;
  Checksum *checksum;
  proxy_protocol::read_get_object_info_response(response, _status, size,
                                                checksum);
  check_status(__PRETTY_FUNCTION__);
  return tuple<uint64_t, Checksum *>(size, checksum);
}

void Proxy_client::read_objects_slices(
    const string &namespace_,
    const vector<proxy_protocol::ObjectSlices> &slices,
    const consistent_read consistent_read) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_read_objects_slices_request(
      mb, namespace_, slices, BooleanEnumTrue(consistent_read));
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_read_objects_slices_response(response, _status, slices);

  check_status(__PRETTY_FUNCTION__);
}

void Proxy_client::invalidate_cache(const string &namespace_) {
  _stream.expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_invalidate_cache_request(mb, namespace_);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_invalidate_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

void Proxy_client::drop_cache(const string &namespace_) {
  _stream.expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_drop_cache_request(mb, namespace_);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_drop_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

std::tuple<int32_t, int32_t, int32_t, std::string>
Proxy_client::get_proxy_version() {
  _stream.expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_get_proxy_version_request(mb);
  mb.output(_stream);
  message response(_stream);
  std::tuple<int32_t, int32_t, int32_t, std::string> result;
  int32_t &major = std::get<0>(result);
  int32_t &minor = std::get<1>(result);
  int32_t &patch = std::get<2>(result);
  std::string &hash = std::get<3>(result);
  proxy_protocol::read_get_proxy_version_response(response, _status, major,
                                                  minor, patch, hash);
  check_status(__PRETTY_FUNCTION__);

  return result;
}
}
}
