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

#include "generic_proxy_client.h"
#include "tcp_proxy_client.h"
#include "rdma_proxy_client.h"
#include "alba_logger.h"

#include <iostream>

#include <errno.h>
#include <boost/lexical_cast.hpp>

namespace alba {
namespace proxy_client {

using std::string;
using std::vector;
using std::tuple;
using boost::optional;
using llio::message;
using llio::message_builder;

GenericProxy_client::GenericProxy_client(const boost::asio::time_traits<
    boost::posix_time::ptime>::duration_type &expiry_time)
    : _expiry_time(expiry_time) {}

bool GenericProxy_client::namespace_exists(const string &name) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_namespace_exists_request(mb, name);
  _output(mb);

  message response = _input();
  bool exists;
  proxy_protocol::read_namespace_exists_response(response, _status, exists);

  check_status(__PRETTY_FUNCTION__);

  return exists;
}

void GenericProxy_client::create_namespace(
    const string &name, const boost::optional<string> &preset_name) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_create_namespace_request(mb, name, preset_name);
  _output(mb);

  message response = _input();
  proxy_protocol::read_create_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::delete_namespace(const string &name) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_delete_namespace_request(mb, name);
  _output(mb);

  message response = _input();
  proxy_protocol::read_delete_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

tuple<vector<string>, has_more> GenericProxy_client::list_namespaces(
    const string &first, const include_first finc, const optional<string> &last,
    const include_last linc, const int max, const reverse reverse) {

  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_list_namespaces_request(
      mb, first, BooleanEnumTrue(finc), last, BooleanEnumTrue(linc), max,
      BooleanEnumTrue(reverse));
  _output(mb);

  message response = _input();
  std::vector<string> namespaces;
  bool has_more_;
  proxy_protocol::read_list_namespaces_response(response, _status, namespaces,
                                                has_more_);

  check_status(__PRETTY_FUNCTION__);

  return tuple<vector<string>, has_more>(namespaces, has_more(has_more_));
}

void GenericProxy_client::write_object_fs(const string &namespace_,
                                          const string &object_name,
                                          const string &input_file,
                                          const allow_overwrite allow_overwrite,
                                          const Checksum *checksum) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_write_object_fs_request(
      mb, namespace_, object_name, input_file, BooleanEnumTrue(allow_overwrite),
      checksum);
  _output(mb);

  message response = _input();
  proxy_protocol::read_write_object_fs_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::read_object_fs(const string &namespace_,
                                         const string &object_name,
                                         const string &dest_file,
                                         const consistent_read consistent_read,
                                         const should_cache should_cache) {

  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_read_object_fs_request(
      mb, namespace_, object_name, dest_file, BooleanEnumTrue(consistent_read),
      BooleanEnumTrue(should_cache));
  _output(mb);

  message response = _input();
  proxy_protocol::read_read_object_fs_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::delete_object(const string &namespace_,
                                        const string &object_name,
                                        const may_not_exist may_not_exist) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_delete_object_request(mb, namespace_, object_name,
                                              BooleanEnumTrue(may_not_exist));
  _output(mb);

  message response = _input();
  proxy_protocol::read_delete_object_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

tuple<vector<string>, has_more> GenericProxy_client::list_objects(
    const string &namespace_, const string &first, const include_first finc,
    const optional<string> &last, const include_last linc, const int max,
    const reverse reverse) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_list_objects_request(
      mb, namespace_, first, BooleanEnumTrue(finc), last, BooleanEnumTrue(linc),
      max, BooleanEnumTrue(reverse));
  _output(mb);

  message response = _input();
  std::vector<string> objects;
  bool has_more_;
  proxy_protocol::read_list_objects_response(response, _status, objects,
                                             has_more_);

  check_status(__PRETTY_FUNCTION__);
  return tuple<vector<string>, has_more>(objects, has_more(has_more_));
}

void GenericProxy_client::read_objects_slices(
    const string &namespace_,
    const vector<proxy_protocol::ObjectSlices> &slices,
    const consistent_read consistent_read) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_read_objects_slices_request(
      mb, namespace_, slices, BooleanEnumTrue(consistent_read));
  _output(mb);

  message response = _input();
  proxy_protocol::read_read_objects_slices_response(response, _status, slices);

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::write_object_fs2(
    const string &namespace_, const string &object_name,
    const string &input_file, const allow_overwrite allow_overwrite,
    const Checksum *checksum, proxy_protocol::Manifest &mf) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_write_object_fs2_request(
      mb, namespace_, object_name, input_file, BooleanEnumTrue(allow_overwrite),
      checksum);
  _output(mb);

  message response = _input();
  proxy_protocol::read_write_object_fs2_response(response, _status, mf);
  check_status(__PRETTY_FUNCTION__);
}

tuple<uint64_t, Checksum *> GenericProxy_client::get_object_info(
    const string &namespace_, const string &object_name,
    const consistent_read consistent_read, const should_cache should_cache) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_get_object_info_request(
      mb, namespace_, object_name, BooleanEnumTrue(consistent_read),
      BooleanEnumTrue(should_cache));
  _output(mb);

  message response = _input();
  uint64_t size;
  Checksum *checksum;
  proxy_protocol::read_get_object_info_response(response, _status, size,
                                                checksum);
  check_status(__PRETTY_FUNCTION__);
  return tuple<uint64_t, Checksum *>(size, checksum);
}

void GenericProxy_client::invalidate_cache(const string &namespace_) {
  _expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_invalidate_cache_request(mb, namespace_);
  _output(mb);

  message response = _input();
  proxy_protocol::read_invalidate_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::drop_cache(const string &namespace_) {
  _expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_drop_cache_request(mb, namespace_);
  _output(mb);

  message response = _input();
  proxy_protocol::read_drop_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

std::tuple<int32_t, int32_t, int32_t, std::string>
GenericProxy_client::get_proxy_version() {
  _expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_get_proxy_version_request(mb);
  _output(mb);
  message response = _input();

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

double GenericProxy_client::ping(double delay) {
  _expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_ping_request(mb, delay);
  _output(mb);
  message response = _input();
  double result;
  proxy_protocol::read_ping_response(response, _status, result);
  check_status(__PRETTY_FUNCTION__);
  return result;
}

void GenericProxy_client::osd_info(
    std::vector<std::pair<osd_t, proxy_protocol::info_caps>> &result) {
  _expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_osd_info_request(mb);
  _output(mb);
  message response = _input();
  proxy_protocol::read_osd_info_response(response, _status, result);
}
}
}
