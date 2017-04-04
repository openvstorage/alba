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
#include "alba_logger.h"

#include <iostream>

#include <boost/lexical_cast.hpp>
#include <errno.h>

namespace alba {
namespace proxy_client {

using std::string;
using std::vector;
using std::tuple;
using boost::optional;
using llio::message;
using llio::message_builder;

GenericProxy_client::GenericProxy_client(
    const std::chrono::steady_clock::duration &timeout,
    std::unique_ptr<transport::Transport> &&transport)
    : _transport(transport.release()), _timeout(timeout) {
  init_();
}

void GenericProxy_client::init_() {
  _transport->expires_from_now(_timeout);

  int32_t magic{1148837403};
  int32_t version{1};

  _transport->write_exact((const char *)(&magic), 4);
  _transport->write_exact((const char *)(&version), 4);

  _transport->expires_from_now(std::chrono::steady_clock::duration::max());
}

void GenericProxy_client::_expires_from_now(
    const std::chrono::steady_clock::duration &timeout) {
  _transport->expires_from_now(timeout);
}
void GenericProxy_client::_output() {
  _transport->output(_mb);
  _mb.reset();
}
llio::message GenericProxy_client::_input() {
  return _transport->read_message();
}

void GenericProxy_client::check_status(const char *function_name) {
  _transport->expires_from_now(std::chrono::steady_clock::duration::max());
  if (not _status.is_ok()) {
    ALBA_LOG(DEBUG, function_name << " received rc:"
                                  << (uint32_t)_status._return_code)
    throw proxy_exception(_status._return_code, _status._what);
  }
}

bool GenericProxy_client::namespace_exists(const string &name) {
  _expires_from_now(_timeout);

  proxy_protocol::write_namespace_exists_request(_mb, name);
  _output();

  message response = _input();
  bool exists;
  proxy_protocol::read_namespace_exists_response(response, _status, exists);

  check_status(__PRETTY_FUNCTION__);

  return exists;
}

void GenericProxy_client::create_namespace(
    const string &name, const boost::optional<string> &preset_name) {
  _expires_from_now(_timeout);

  proxy_protocol::write_create_namespace_request(_mb, name, preset_name);
  _output();

  message response = _input();
  proxy_protocol::read_create_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::delete_namespace(const string &name) {
  _expires_from_now(_timeout);

  proxy_protocol::write_delete_namespace_request(_mb, name);
  _output();

  message response = _input();
  proxy_protocol::read_delete_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

tuple<vector<string>, has_more> GenericProxy_client::list_namespaces(
    const string &first, const include_first finc, const optional<string> &last,
    const include_last linc, const int max, const reverse reverse) {

  _expires_from_now(_timeout);

  proxy_protocol::write_list_namespaces_request(
      _mb, first, BooleanEnumTrue(finc), last, BooleanEnumTrue(linc), max,
      BooleanEnumTrue(reverse));
  _output();

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
  _expires_from_now(_timeout);

  proxy_protocol::write_write_object_fs_request(
      _mb, namespace_, object_name, input_file,
      BooleanEnumTrue(allow_overwrite), checksum);
  _output();

  message response = _input();
  proxy_protocol::read_write_object_fs_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::read_object_fs(const string &namespace_,
                                         const string &object_name,
                                         const string &dest_file,
                                         const consistent_read consistent_read,
                                         const should_cache should_cache) {

  _expires_from_now(_timeout);

  proxy_protocol::write_read_object_fs_request(
      _mb, namespace_, object_name, dest_file, BooleanEnumTrue(consistent_read),
      BooleanEnumTrue(should_cache));
  _output();

  message response = _input();
  proxy_protocol::read_read_object_fs_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::delete_object(const string &namespace_,
                                        const string &object_name,
                                        const may_not_exist may_not_exist) {
  _expires_from_now(_timeout);

  proxy_protocol::write_delete_object_request(_mb, namespace_, object_name,
                                              BooleanEnumTrue(may_not_exist));
  _output();

  message response = _input();
  proxy_protocol::read_delete_object_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

tuple<vector<string>, has_more> GenericProxy_client::list_objects(
    const string &namespace_, const string &first, const include_first finc,
    const optional<string> &last, const include_last linc, const int max,
    const reverse reverse) {
  _expires_from_now(_timeout);

  proxy_protocol::write_list_objects_request(
      _mb, namespace_, first, BooleanEnumTrue(finc), last,
      BooleanEnumTrue(linc), max, BooleanEnumTrue(reverse));
  _output();

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
    const consistent_read consistent_read,
    alba::statistics::RoraCounter &cntr) {

  if (slices.size() == 0) {
    return;
  }

  _expires_from_now(_timeout);

  proxy_protocol::write_read_objects_slices_request(
      _mb, namespace_, slices, BooleanEnumTrue(consistent_read));
  _output();

  message response = _input();
  proxy_protocol::read_read_objects_slices_response(response, _status, slices);
  cntr.slow_path += slices.size();

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::read_objects_slices2(
    const string &namespace_,
    const vector<proxy_protocol::ObjectSlices> &slices,
    const consistent_read consistent_read,
    vector<proxy_protocol::object_info> &object_infos,
    alba::statistics::RoraCounter &cntr) {

  if (slices.size() == 0) {
    return;
  }
  _expires_from_now(_timeout);

  proxy_protocol::write_read_objects_slices2_request(
      _mb, namespace_, slices, BooleanEnumTrue(consistent_read));
  _output();

  message response = _input();
  proxy_protocol::read_read_objects_slices2_response(response, _status, slices,
                                                     object_infos);
  cntr.slow_path += slices.size();

  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::write_object_fs2(
    const string &namespace_, const string &object_name,
    const string &input_file, const allow_overwrite allow_overwrite,
    const Checksum *checksum, proxy_protocol::ManifestWithNamespaceId &mf) {
  _expires_from_now(_timeout);

  proxy_protocol::write_write_object_fs2_request(
      _mb, namespace_, object_name, input_file,
      BooleanEnumTrue(allow_overwrite), checksum);
  _output();

  message response = _input();
  proxy_protocol::read_write_object_fs2_response(response, _status, mf);
  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::update_session(
    const std::vector<std::pair<std::string, boost::optional<std::string>>>
        &args,
    std::vector<std::pair<std::string, std::string>> &processed_kvs) {
  _expires_from_now(_timeout);

  proxy_protocol::write_update_session_request(_mb, args);
  _output();

  message response = _input();
  proxy_protocol::read_update_session_response(response, _status,
                                               processed_kvs);
  check_status(__PRETTY_FUNCTION__);
}
tuple<uint64_t, Checksum *> GenericProxy_client::get_object_info(
    const string &namespace_, const string &object_name,
    const consistent_read consistent_read, const should_cache should_cache) {
  _expires_from_now(_timeout);

  proxy_protocol::write_get_object_info_request(
      _mb, namespace_, object_name, BooleanEnumTrue(consistent_read),
      BooleanEnumTrue(should_cache));
  _output();

  message response = _input();
  uint64_t size;
  Checksum *checksum;
  proxy_protocol::read_get_object_info_response(response, _status, size,
                                                checksum);
  check_status(__PRETTY_FUNCTION__);
  return tuple<uint64_t, Checksum *>(size, checksum);
}

void GenericProxy_client::apply_sequence(
    const string &namespace_, const write_barrier write_barrier,
    const vector<std::shared_ptr<sequences::Assert>> &asserts,
    const vector<std::shared_ptr<sequences::Update>> &updates) {
  std::vector<proxy_protocol::object_info> object_infos;
  apply_sequence_(namespace_, write_barrier, asserts, updates, object_infos);
}

void GenericProxy_client::apply_sequence_(
    const string &namespace_, const write_barrier write_barrier,
    const vector<std::shared_ptr<sequences::Assert>> &asserts,
    const vector<std::shared_ptr<sequences::Update>> &updates,
    std::vector<proxy_protocol::object_info> &object_infos) {
  _expires_from_now(_timeout);

  proxy_protocol::write_apply_sequence_request(
      _mb, namespace_, BooleanEnumTrue(write_barrier), asserts, updates);
  _output();

  message response = _input();
  proxy_protocol::read_apply_sequence_response(response, _status, object_infos);
  check_status(__PRETTY_FUNCTION__);
  return;
}

void GenericProxy_client::invalidate_cache(const string &namespace_) {
  _expires_from_now(_timeout);
  proxy_protocol::write_invalidate_cache_request(_mb, namespace_);
  _output();

  message response = _input();
  proxy_protocol::read_invalidate_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

void GenericProxy_client::drop_cache(const string &namespace_) {
  _expires_from_now(_timeout);
  proxy_protocol::write_drop_cache_request(_mb, namespace_);
  _output();

  message response = _input();
  proxy_protocol::read_drop_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

std::tuple<int32_t, int32_t, int32_t, std::string>
GenericProxy_client::get_proxy_version() {
  _expires_from_now(_timeout);

  proxy_protocol::write_get_proxy_version_request(_mb);
  _output();
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
  _expires_from_now(_timeout);

  proxy_protocol::write_ping_request(_mb, delay);
  _output();
  message response = _input();
  double result;
  proxy_protocol::read_ping_response(response, _status, result);
  check_status(__PRETTY_FUNCTION__);
  return result;
}

void GenericProxy_client::osd_info(osd_map_t &result) {
  _expires_from_now(_timeout);

  proxy_protocol::write_osd_info_request(_mb);
  _output();
  message response = _input();
  proxy_protocol::read_osd_info_response(response, _status, result);
}

void GenericProxy_client::osd_info2(osd_maps_t &result) {
  ALBA_LOG(DEBUG, "Generic_proxy_client::osd_info2");
  _expires_from_now(_timeout);

  proxy_protocol::write_osd_info2_request(_mb);
  _output();
  message response = _input();
  proxy_protocol::read_osd_info2_response(response, _status, result);
}

bool GenericProxy_client::has_local_fragment_cache() {
  _expires_from_now(_timeout);

  proxy_protocol::write_has_local_fragment_cache_request(_mb);
  _output();
  message response = _input();
  bool result;
  proxy_protocol::read_has_local_fragment_cache_response(response, _status,
                                                         result);
  check_status(__PRETTY_FUNCTION__);
  return result;
}

boost::optional<string> GenericProxy_client::get_fragment_encryption_key(
    const string &alba_id, const namespace_t namespace_id) {
  _expires_from_now(_timeout);

  proxy_protocol::write_get_fragment_encryption_key_request(_mb, alba_id,
                                                            namespace_id);
  _output();
  message response = _input();
  boost::optional<string> enc_key;
  proxy_protocol::read_get_fragment_encryption_key_response(response, _status,
                                                            enc_key);
  check_status(__PRETTY_FUNCTION__);
  return enc_key;
}
}
}
