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

#include "tcp_transport.h"

namespace alba {
namespace transport {

using std::string;
using std::vector;
using std::tuple;
using boost::optional;
using llio::message;
using llio::message_builder;

TCP_transport::TCP_transport(
    const string &ip, const string &port,
    const std::chrono::steady_clock::duration &timeout) {
  ALBA_LOG(INFO, "TCP_transport(" << ip << ", " << port << ")");
  this->expires_from_now(timeout);
  _stream.connect(ip, port);

  const boost::asio::ip::tcp::no_delay option(true);
  _stream.rdbuf()->set_option(option);

  _stream.expires_at(boost::posix_time::max_date_time);
}

void TCP_transport::expires_from_now(
    const std::chrono::steady_clock::duration &timeout) {
  _stream.expires_from_now(boost::posix_time::milliseconds(
      std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count()));
}

void TCP_transport::write_exact(const char *buf, int len) {
  _stream.write(buf, len);
  _stream.flush();
  if (!_stream.good()) {
    throw transport_exception("invalid outputstream");
  }
}
void TCP_transport::read_exact(char *buf, int len) {
  _stream.read(buf, len);
  if (!_stream.good()) {
    throw transport_exception("invalid inputstream");
  }
}
}
}
