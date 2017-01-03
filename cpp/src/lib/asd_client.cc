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

namespace alba {
namespace asd_client {

using llio::message_builder;
using llio::message;

Asd_client::Asd_client(const std::chrono::steady_clock::duration &timeout,
                       std::unique_ptr<transport::Transport> &&transport)
    : _transport(transport.release()), _timeout(timeout) {}

void Asd_client::init(boost::optional<string> long_id) {
  _transport->expires_from_now(_timeout);

  message_builder mb;
  asd_protocol::make_prologue(mb, long_id);
  string prologue = mb.as_string();
  _transport->write_exact(&(prologue.data())[4], prologue.length() - 4);

  uint32_t rc;
  _transport->read_exact((char *)&rc, 4);

  if (rc != 0) {
    throw asd_exception("error during asd prologue");
  }

  uint32_t length;
  _transport->read_exact((char *)&length, 4);

  std::vector<char> buf(length);
  _transport->read_exact(buf.data(), length);
  string long_id2(buf.data(), length);

  if (long_id != boost::none) {
    if (*long_id != long_id2) {
      throw asd_exception("wrong asd on the other side");
    }
  }
}

void Asd_client::partial_get(string &key, vector<slice> &slices) {
  _transport->expires_from_now(_timeout);

  message_builder mb;
  asd_protocol::write_partial_get_request(mb, key, slices);
  _transport->output(mb);

  message response = _transport->read_message();
  bool success;
  asd_protocol::read_partial_get_response(response, _status, success);

  if (!success) {
    throw asd_exception("partial read request failed");
  }

  for (auto &slice : slices) {
    _transport->read_exact((char *)slice.target, slice.length);
  }
}
}
}
