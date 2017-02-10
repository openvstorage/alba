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
#include <boost/lambda/bind.hpp>
#include <boost/lambda/lambda.hpp>

namespace alba {
namespace transport {

using std::string;
using std::vector;
using std::tuple;
using boost::optional;
using llio::message;
using llio::message_builder;
using namespace boost::asio;

#define _convert(t)                                                            \
  boost::posix_time::milliseconds(                                             \
      std::chrono::duration_cast<std::chrono::milliseconds>(t).count())
#define _NEVER boost::posix_time::pos_infin

/*
  using boost::asio::ip::tcp::iostream is comfy,
  but results in using (hardcoded buffers of size 512),
  and thus way too many syscalls for reading 4KB.

  So we move to boost::asio::ip::tcp::socket

 */

TCP_transport::TCP_transport(const string &ip, const string &port,
                             const std::chrono::steady_clock::duration &timeout)
    : _io_service{}, _socket(_io_service), _deadline(_io_service),
      _timeout(_convert(timeout)) {
  ALBA_LOG(INFO, "TCP_transport(" << ip << ", " << port << ")");
  _deadline.expires_from_now(_timeout);
  _check_deadline();

  int port_as_int = std::stoi(port);
  auto addr = ip::address::from_string(ip);
  auto sa = ip::tcp::endpoint(addr, port_as_int);

  boost::system::error_code ec = boost::asio::error::would_block;
  auto handler = [&](const boost::system::error_code &x) -> void { ec = x; };
  _socket.async_connect(sa, handler);

  do
    _io_service.run_one();
  while (ec == boost::asio::error::would_block);

  if (ec || !_socket.is_open())
    throw boost::system::system_error(
        ec ? ec : boost::asio::error::operation_aborted);

  const ip::tcp::no_delay no_delay(true);
  _socket.set_option(no_delay);
  _deadline.expires_at(_NEVER);
}

void TCP_transport::expires_from_now(
    const std::chrono::steady_clock::duration &timeout) {
  _timeout = _convert(timeout);
}

void TCP_transport::write_exact(const char *buf, int len) {
  boost::asio::const_buffers_1 buffer(buf, len);
  // boost::asio::write(_socket, buffer);

  //_io_service.reset();
  _deadline.expires_from_now(_timeout);
  boost::system::error_code ec = boost::asio::error::would_block;

  auto handler = [&](const boost::system::error_code &x,
                     std::size_t /*len */) -> void { ec = x; };
  boost::asio::async_write(_socket, buffer, handler);

  do {
    _io_service.run_one();
  } while (ec == boost::asio::error::would_block);

  if (ec)
    throw boost::system::system_error(ec);
}

void TCP_transport::read_exact(char *buf, int len) {
  boost::asio::mutable_buffers_1 buffer(buf, len);
  // boost::asio::read(_socket, buffer);

  //_io_service.reset();
  _deadline.expires_from_now(_timeout);
  boost::system::error_code ec = boost::asio::error::would_block;

  auto handler = [&](const boost::system::error_code &x,
                     std::size_t /* len*/) -> void { ec = x; };
  boost::asio::async_read(_socket, buffer, handler);

  // Block until the asynchronous operation has completed.

  do {
    _io_service.run_one();
  } while (ec == boost::asio::error::would_block);

  if (ec)
    throw boost::system::system_error(ec);
}

void TCP_transport::_check_deadline() {
  if (_deadline.expires_at() <= deadline_timer::traits_type::now()) {
    boost::system::error_code ignored_ec;
    _socket.close(ignored_ec);
    _deadline.expires_at(_NEVER);
  }
  _deadline.async_wait(
      boost::lambda::bind(&TCP_transport::_check_deadline, this));
}

TCP_transport::~TCP_transport() {
  boost::system::error_code ec;
  _socket.close(ec); // does not throw
}
}
}
