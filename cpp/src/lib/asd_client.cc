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

#include "asd_client.h"
#include <thread>

namespace alba {
namespace asd_client {

using llio::message_builder;
using llio::message;

void Asd_client::check_status(const char *function_name) {
  _transport -> expires_from_now(std::chrono::steady_clock::duration::max());
  if (not _status.is_ok()) {
    ALBA_LOG(DEBUG, function_name << " received rc:"
                                  << (uint32_t)_status._return_code)
     throw asd_exception(_status._return_code, function_name);
  }
}


Asd_client::Asd_client(const std::chrono::steady_clock::duration &timeout,
                       std::unique_ptr<transport::Transport> &&transport,
                       boost::optional<string> long_id)
    : _transport(transport.release()), _timeout(timeout) {
  init_(long_id);
}

void Asd_client::init_(boost::optional<string> long_id) {
  _transport->expires_from_now(_timeout);

  message_builder mb;
  asd_protocol::make_prologue(mb, long_id);
  string prologue = mb.as_string();
  _transport->write_exact(&(prologue.data())[4], prologue.length() - 4);

  uint32_t rc;
  _transport->read_exact((char *)&rc, 4);

  if (rc != 0) {
      throw asd_exception(rc, "error during asd prologue");
  }

  uint32_t length;
  _transport->read_exact((char *)&length, 4);

  std::vector<char> buf(length);
  _transport->read_exact(buf.data(), length);
  string long_id2(buf.data(), length);

  if (long_id != boost::none) {
    if (*long_id != long_id2) {
      ALBA_LOG(INFO, "expected asd with id " << *long_id << " but found "
                                             << long_id2 << " instead");
      throw asd_exception(0xff, "wrong asd on the other side");
    }
  }
}

void Asd_client::partial_get(string &key, vector<slice> &slices) {
  _transport->expires_from_now(_timeout);

  asd_protocol::write_partial_get_request(_mb, key, slices);
  _transport->output(_mb);
  _mb.reset();
  message response = _transport->read_message();
  bool success;
  asd_protocol::read_partial_get_response(response, _status, success);

  check_status(__PRETTY_FUNCTION__);

  for (auto &slice : slices) {
    _transport->read_exact((char *)slice.target, slice.length);
  }
}

void Asd_client::set_slowness(asd_protocol::slowness_t &slowness){
  _transport -> expires_from_now(_timeout);
  asd_protocol::write_set_slowness_request(_mb, slowness);
  _transport -> output(_mb);
  _mb.reset();
  message response = _transport-> read_message();
  asd_protocol::read_set_slowness_response(response, _status);

}

std::tuple<int32_t, int32_t, int32_t, std::string>
Asd_client::get_version(){
  _transport -> expires_from_now(_timeout);

  asd_protocol::write_get_version_request(_mb);
  _transport -> output(_mb);
  _mb.reset();

  message response = _transport -> read_message();

  std::tuple<int32_t, int32_t, int32_t, std::string> result;
  int32_t &major = std::get<0>(result);
  int32_t &minor = std::get<1>(result);
  int32_t &patch = std::get<2>(result);
  std::string &hash = std::get<3>(result);
  asd_protocol::read_get_version_response(response, _status, major,
                                              minor, patch, hash);
  check_status(__PRETTY_FUNCTION__);
  return result;
}
}
}
