/*
  Copyright (C) 2016 iNuron NV

  This file is part of Open vStorage Open Source Edition (OSE), as available
  from


  http://www.openvstorage.org and
  http://www.openvstorage.com.

  This file is free software; you can redistribute it and/or modify it
  under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
  as published by the Free Software Foundation, in version 3 as it comes
  in the <LICENSE.txt> file of the Open vStorage OSE distribution.

  Open vStorage is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY of any kind.
*/

#include "tcp_proxy_client.h"
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
    : GenericProxy_client(expiry_time) {
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
  _expires_from_now(boost::posix_time::max_date_time);
  if (not _status.is_ok()) {
    ALBA_LOG(DEBUG, function_name
                        << " received rc:" << (uint32_t)_status._return_code)
    throw proxy_exception(_status._return_code, _status._what);
  }
}

void TCPProxy_client::_expires_from_now(const boost::asio::time_traits<
    boost::posix_time::ptime>::duration_type &expiry_time) {
  _stream.expires_from_now(expiry_time);
}

void TCPProxy_client::_output(message_builder &mb) { mb.output(_stream); }

message TCPProxy_client::_input() {
  message response(_stream);
  return response;
}
}
}
