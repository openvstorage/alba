/*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use thi>s file except in compliance with the License.
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

#include <iostream>

#include <rdma/rsocket.h>
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

TCPProxy_client::TCPProxy_client(
    const string &ip, const string &port,
    const boost::asio::time_traits<boost::posix_time::ptime>::duration_type &
        expiry_time)
    : _status(), _expiry_time(expiry_time) {
  ALBA_LOG(INFO, "TCPProxy_client(" << ip << ", " << port << ")");
  _stream.expires_from_now(_expiry_time);
  _stream.connect(ip, port);
  int32_t magic{1148837403};
  int32_t version{1};
  _stream.write((const char *)(&magic), 4);
  _stream.write((const char *)(&version), 4);
  _stream.expires_at(boost::posix_time::max_date_time);
}

void TCPProxy_client::check_status(const char *function_name) {
  _stream.expires_at(boost::posix_time::max_date_time);
  if (not _status.is_ok()) {
    ALBA_LOG(DEBUG, function_name
                        << " received rc:" << (uint32_t)_status._return_code)
    throw proxy_exception(_status._return_code, _status._what);
  }
}

tuple<vector<string>, has_more> TCPProxy_client::list_namespaces(
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

bool TCPProxy_client::namespace_exists(const string &name) {
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

void TCPProxy_client::create_namespace(
    const string &name, const boost::optional<string> &preset_name) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_create_namespace_request(mb, name, preset_name);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_create_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void TCPProxy_client::delete_namespace(const string &name) {
  _stream.expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_delete_namespace_request(mb, name);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_delete_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

tuple<vector<string>, has_more> TCPProxy_client::list_objects(
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

void TCPProxy_client::read_object_fs(const string &namespace_,
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

void TCPProxy_client::write_object_fs(const string &namespace_,
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

void TCPProxy_client::delete_object(const string &namespace_,
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

tuple<uint64_t, Checksum *> TCPProxy_client::get_object_info(
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

void TCPProxy_client::read_objects_slices(
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

void TCPProxy_client::invalidate_cache(const string &namespace_) {
  _stream.expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_invalidate_cache_request(mb, namespace_);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_invalidate_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

void TCPProxy_client::drop_cache(const string &namespace_) {
  _stream.expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_drop_cache_request(mb, namespace_);
  mb.output(_stream);

  message response(_stream);
  proxy_protocol::read_drop_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

std::tuple<int32_t, int32_t, int32_t, std::string>
TCPProxy_client::get_proxy_version() {
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

double TCPProxy_client::ping(double delay){
  _stream.expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_ping_request(mb,delay);
  mb.output(_stream);
  message response(_stream);
  double result;
  proxy_protocol::read_ping_response(response, _status, result);
  check_status(__PRETTY_FUNCTION__);
  return result;
}

double _stamp_ms(){
  struct timeval tp;
  gettimeofday(&tp, NULL);
  double t0 = 1000 * tp.tv_sec + (double) tp.tv_usec / 1e3;
  return t0;
}
  
void RDMAProxy_client::_really_write(const char *buf, const int len) {
  int flags = 0;
  int sent;
  int todo = len;
  int off = 0;
  int nfds = 1;
  double t0 = _stamp_ms();
  while (todo > 0 && _request_time_left > 0 ) {
    ALBA_LOG(DEBUG, "todo=" << todo
             << ", request_time_left=" << _request_time_left);
    struct pollfd pollfd;
    pollfd.fd = _socket;
    pollfd.events = POLLOUT;
    pollfd.revents = 0;
    int rc = rpoll(&pollfd, nfds, _request_time_left);
    ALBA_LOG(DEBUG, "rc=" << rc);
    if (rc < 0 ) {
      throw proxy_exception(rc, "really_write.rpoll");
    }
    if (rc == 0 ){
      throw proxy_exception(rc, "timeout");
    } 
    sent = rsend(_socket, &buf[off], todo, flags);
    if (sent < 0) {
      throw proxy_exception(sent, "really_write.send");
    }
    off += sent;
    todo -= sent;
    double t1 = _stamp_ms();
    double delta = t1 - t0;
    _request_time_left = _request_time_left - (int) delta;
  }
}



  
void RDMAProxy_client::_really_read(char *buf, const int len) {
  int flags = 0;
  int read = 0;
  int todo = len;
  int off = 0;
  int nfds = 1;
  double t0 = _stamp_ms();
  
  while (todo > 0 && _request_time_left > 0) {
    ALBA_LOG(DEBUG, "todo=" << todo << ", _request_time_left=" << _request_time_left);

    // wait until readable, with timeout.
    struct pollfd pollfd;
    pollfd.fd = _socket;
    pollfd.events = POLLIN;
    pollfd.revents = 0;
    int rc = rpoll(&pollfd, nfds, _request_time_left);
    ALBA_LOG(DEBUG, "rc=" << rc );
    if (rc < 0){
      throw proxy_exception(rc, "really_read.rpoll");
    } 
    if (rc == 0){
      throw proxy_exception(read, "timeout");
    }
    
    read = rrecv(_socket, &buf[off], todo, flags);
    if (read < 0) {
      throw proxy_exception(read, "really_read.rrecv");
    }
    off += read;
    todo -= read;
    double t1 = _stamp_ms();
    double delta = t1 - t0;
    _request_time_left = _request_time_left - (int)delta;
  }
}

void RDMAProxy_client::check_status(const char *function_name) {
  _expires_from_now(boost::posix_time::hours(1));
  if (not _status.is_ok()) {
    ALBA_LOG(DEBUG, function_name
                        << " received rc:" << (uint32_t)_status._return_code)
    throw proxy_exception(_status._return_code, _status._what);
  }
}

RDMAProxy_client::RDMAProxy_client(
    const string &ip, const string &port,
    const boost::asio::time_traits<boost::posix_time::ptime>::duration_type &
    expiry_time) : _status(),
                   _expiry_time(expiry_time),
                   _request_time_left(10000)
{
  
  ALBA_LOG(INFO, "RDMAProxy_client(" << ip << ", " << port << ")");

  int32_t magic{1148837403};
  int32_t version{1};
  _socket = rsocket(AF_INET, SOCK_STREAM, 0);

  _writer = [&](const char *buffer, const int len)
                -> void { _really_write(buffer, len); };

  _reader =
      [&](char *buffer, const int len) -> void { _really_read(buffer, len); };

  if (_socket < 0) {
    throw proxy_exception(-1, "socket?");
  }
  struct sockaddr_in serv_addr;

  serv_addr.sin_family = AF_INET;

  int port_i = boost::lexical_cast<int>(port);
  serv_addr.sin_port = htons(port_i);

  int ok = inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr);
  if (ok < 0) {
    throw proxy_exception(errno, "ip");
  }
  ALBA_LOG(INFO, "connecting");

  // TODO: timeouts on connect.
  ok = rconnect(_socket, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
  if (ok < 0) {
    throw proxy_exception(errno, "connect");
  }

  _really_write((const char *)(&magic), sizeof(int32_t));
  _really_write((const char *)(&version), sizeof(int32_t));
}

tuple<vector<string>, has_more> RDMAProxy_client::list_namespaces(
    const string &first, const include_first finc, const optional<string> &last,
    const include_last linc, const int max, const reverse reverse) {

  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_list_namespaces_request(
      mb, first, BooleanEnumTrue(finc), last, BooleanEnumTrue(linc), max,
      BooleanEnumTrue(reverse));
  mb.output_using([&](const char *buffer, const int len)
                      -> void { _really_write(buffer, len); });

  message response([&](char *buffer, const int len)
                       -> void { _really_read(buffer, len); });
  std::vector<string> namespaces;
  bool has_more_;
  proxy_protocol::read_list_namespaces_response(response, _status, namespaces,
                                                has_more_);

  check_status(__PRETTY_FUNCTION__);

  return tuple<vector<string>, has_more>(namespaces, has_more(has_more_));
}

bool RDMAProxy_client::namespace_exists(const string &name) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_namespace_exists_request(mb, name);
  mb.output_using([&](const char *buffer, const int len)
                      -> void { _really_write(buffer, len); });

  message response([&](char *buffer, const int len)
                       -> void { _really_read(buffer, len); });
  bool exists;
  proxy_protocol::read_namespace_exists_response(response, _status, exists);

  check_status(__PRETTY_FUNCTION__);

  return exists;
}

void RDMAProxy_client::create_namespace(
    const string &name, const boost::optional<string> &preset_name) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_create_namespace_request(mb, name, preset_name);
  mb.output_using(_writer);

  message response(_reader);
  proxy_protocol::read_create_namespace_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void RDMAProxy_client::delete_namespace(const string &name) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_delete_namespace_request(mb, name);
  mb.output_using(_writer);

  message response(_reader);
  proxy_protocol::read_delete_namespace_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

tuple<vector<string>, has_more> RDMAProxy_client::list_objects(
    const string &namespace_, const string &first, const include_first finc,
    const optional<string> &last, const include_last linc, const int max,
    const reverse reverse) {

  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_list_objects_request(
      mb, namespace_, first, BooleanEnumTrue(finc), last, BooleanEnumTrue(linc),
      max, BooleanEnumTrue(reverse));

  mb.output_using(_writer);
  message response(_reader);

  std::vector<string> objects;
  bool has_more_;
  proxy_protocol::read_list_objects_response(response, _status, objects,
                                             has_more_);

  check_status(__PRETTY_FUNCTION__);
  return tuple<vector<string>, has_more>(objects, has_more(has_more_));
}

void RDMAProxy_client::write_object_fs(const string &namespace_,
                                       const string &object_name,
                                       const string &input_file,
                                       const allow_overwrite allow_overwrite,
                                       const Checksum *checksum) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_write_object_fs_request(
      mb, namespace_, object_name, input_file, BooleanEnumTrue(allow_overwrite),
      checksum);
  mb.output_using(_writer);

  message response(_reader);
  proxy_protocol::read_write_object_fs_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

void RDMAProxy_client::delete_object(const string &namespace_,
                                     const string &object_name,
                                     const may_not_exist may_not_exist) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_delete_object_request(mb, namespace_, object_name,
                                              BooleanEnumTrue(may_not_exist));
  mb.output_using(_writer);

  message response(_reader);
  proxy_protocol::read_delete_object_response(response, _status);

  check_status(__PRETTY_FUNCTION__);
}

tuple<uint64_t, Checksum *> RDMAProxy_client::get_object_info(
    const string &namespace_, const string &object_name,
    const consistent_read consistent_read, const should_cache should_cache) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_get_object_info_request(
      mb, namespace_, object_name, BooleanEnumTrue(consistent_read),
      BooleanEnumTrue(should_cache));
  mb.output_using(_writer);

  message response(_reader);
  uint64_t size;
  Checksum *checksum;
  proxy_protocol::read_get_object_info_response(response, _status, size,
                                                checksum);
  check_status(__PRETTY_FUNCTION__);
  return tuple<uint64_t, Checksum *>(size, checksum);
}

void RDMAProxy_client::read_object_fs(const string &namespace_,
                                      const string &object_name,
                                      const string &dest_file,
                                      const consistent_read consistent_read,
                                      const should_cache should_cache) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_read_object_fs_request(
      mb, namespace_, object_name, dest_file, BooleanEnumTrue(consistent_read),
      BooleanEnumTrue(should_cache));
  mb.output_using(_writer);
  message response(_reader);
  proxy_protocol::read_read_object_fs_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

void RDMAProxy_client::read_objects_slices(
    const string &namespace_,
    const vector<proxy_protocol::ObjectSlices> &slices,
    const consistent_read consistent_read) {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_read_objects_slices_request(
      mb, namespace_, slices, BooleanEnumTrue(consistent_read));
  mb.output_using(_writer);

  message response(_reader);
  proxy_protocol::read_read_objects_slices_response(response, _status, slices);

  check_status(__PRETTY_FUNCTION__);
}

void RDMAProxy_client::invalidate_cache(const string &namespace_) {
  _expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_invalidate_cache_request(mb, namespace_);
  mb.output_using(_writer);

  message response(_reader);
  proxy_protocol::read_invalidate_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

void RDMAProxy_client::drop_cache(const string &namespace_) {
  _expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_drop_cache_request(mb, namespace_);
  mb.output_using(_writer);

  message response(_reader);
  proxy_protocol::read_drop_cache_response(response, _status);
  check_status(__PRETTY_FUNCTION__);
}

std::tuple<int32_t, int32_t, int32_t, std::string>
RDMAProxy_client::get_proxy_version() {
  _expires_from_now(_expiry_time);

  message_builder mb;
  proxy_protocol::write_get_proxy_version_request(mb);
  mb.output_using(_writer);

  message response(_reader);
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

void RDMAProxy_client::_expires_from_now(const boost::asio::time_traits<
                       boost::posix_time::ptime>::duration_type &expiry_time){
  _request_time_left = expiry_time.total_milliseconds();
  ALBA_LOG(DEBUG, "RDMAProxy_client::_expires_from_now("
           << _request_time_left
           << " ms)");
}
  
double RDMAProxy_client::ping(const double delay){
  _expires_from_now(_expiry_time);
  message_builder mb;
  proxy_protocol::write_ping_request(mb, delay);
  mb.output_using(_writer);
  message response(_reader);
  double result;
  proxy_protocol::read_ping_response(response, _status, result);
  check_status(__PRETTY_FUNCTION__);
  return result;
}

RDMAProxy_client::~RDMAProxy_client() {
  ALBA_LOG(INFO, "~RDMAProxy_client");
  int r = rclose(_socket);
  if (r < 0) {
    ALBA_LOG(INFO, "exception in close: fd:" << _socket << " r=" << r);
  }
}

std::unique_ptr<Proxy_client> make_proxy_client(
    const std::string &ip, const std::string &port,
    const boost::asio::time_traits<boost::posix_time::ptime>::duration_type &
        expiry_time,
    const Transport &transport) {
  Proxy_client *r = NULL;

  switch (transport) {
  case Transport::tcp: {
    r = new TCPProxy_client(ip, port, expiry_time);
  }; break;
  case Transport::rdma: {
    r = new RDMAProxy_client(ip, port, expiry_time);
  }; break;
  }
  std::unique_ptr<Proxy_client> result(r);
  return result;
}

std::ostream &operator<<(std::ostream &os, Transport t) {
  switch (t) {
  case Transport::tcp:
    os << "TCP";
    break;
  case Transport::rdma:
    os << "RDMA";
    break;
  }

  return os;
}

std::istream &operator>>(std::istream &is, Transport &t) {
  std::string s;
  is >> s;
  if (s == "TCP") {
    t = Transport::tcp;
  } else if (s == "RDMA") {
    t = Transport::rdma;
  } else {
    is.setstate(std::ios_base::failbit);
  }

  return is;
}
}
}
