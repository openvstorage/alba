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
#include "rdma_proxy_client.h"
#include <rdma/rsocket.h>
#include <boost/lexical_cast.hpp>

namespace alba {
namespace proxy_client {

using std::string;
using llio::message;

std::string _build_msg(const std::string &prefix) {
  int _errno = errno;
  std::ostringstream ss;
  ss << prefix << " " << _errno;
  return ss.str();
}

void RDMAProxy_client::_really_write(const char *buf, const int len) {
  if (len == 0) {
    return;
  }
  if (len < 0) {
    throw proxy_exception(len, _build_msg("really_write negative length"));
  }

  int flags = 0;
  int sent;
  int todo = len;
  int off = 0;
  int nfds = 1;

  std::chrono::duration<long int, std::nano> time_remaining =
      _deadline - std::chrono::steady_clock::now();

  do {
    ALBA_LOG(DEBUG, "todo=" << todo << ", time_remaining.count()="
                            << time_remaining.count());

    // wait until writeable, with timeout
    struct pollfd pollfd;
    pollfd.fd = _socket;
    pollfd.events = POLLOUT;
    pollfd.revents = 0;
    int rc = rpoll(&pollfd, nfds,
                   std::chrono::duration_cast<std::chrono::milliseconds>(
                       time_remaining).count());
    ALBA_LOG(DEBUG, "rc=" << rc);
    if (rc < 0) {
      throw proxy_exception(rc, _build_msg("really_write.rpoll:"));
    }
    if (rc == 0) {
      throw proxy_exception(rc, "really_write.rpoll timeout");
    }

    sent = rsend(_socket, &buf[off], todo, flags);
    if (sent < 0) {
      throw proxy_exception(sent, _build_msg("really_write.send"));
    }
    off += sent;
    todo -= sent;
  } while (todo > 0 &&
           (time_remaining = _deadline - std::chrono::steady_clock::now())
                   .count() > 0);
}

void RDMAProxy_client::_really_read(char *buf, const int len) {
  if (len == 0) {
    return;
  }
  if (len < 0) {
    throw proxy_exception(len, _build_msg("really_read negative length"));
  }

  int flags = 0;
  int read = 0;
  int todo = len;
  int off = 0;
  int nfds = 1;

  std::chrono::duration<long int, std::nano> time_remaining =
      _deadline - std::chrono::steady_clock::now();

  do {

    ALBA_LOG(DEBUG, "todo=" << todo << ", time_remaining.count()="
                            << time_remaining.count());

    // wait until readable, with timeout.
    struct pollfd pollfd;
    pollfd.fd = _socket;
    pollfd.events = POLLIN;
    pollfd.revents = 0;
    int rc = rpoll(&pollfd, nfds,
                   std::chrono::duration_cast<std::chrono::milliseconds>(
                       time_remaining).count());
    ALBA_LOG(DEBUG, "rc=" << rc);
    if (rc < 0) {
      throw proxy_exception(rc, _build_msg("really_read.rpoll"));
    }
    if (rc == 0) {
      throw proxy_exception(read, "really_read.rpoll timeout");
    }

    read = rrecv(_socket, &buf[off], todo, flags);
    if (read <= 0) {
      throw proxy_exception(read, _build_msg("really_read.rrecv=" + read));
    }
    off += read;
    todo -= read;

  } while (todo > 0 &&
           (time_remaining = _deadline - std::chrono::steady_clock::now())
                   .count() > 0);
}

void RDMAProxy_client::check_status(const char *function_name) {
  _expires_from_now(std::chrono::hours(1));
  if (not _status.is_ok()) {
    ALBA_LOG(DEBUG, function_name
                        << " received rc:" << (uint32_t)_status._return_code)
    throw proxy_exception(_status._return_code, _status._what);
  }
}

RDMAProxy_client::RDMAProxy_client(
    const string &ip, const string &port,
    const std::chrono::steady_clock::duration &timeout)
    : GenericProxy_client(timeout) {

  _expires_from_now(timeout);

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

  // make it a non-blocking rsocket
  int retcode;
  retcode = rfcntl(_socket, F_GETFL, 0);
  if (retcode == -1 || rfcntl(_socket, F_SETFL, retcode | O_NONBLOCK) == -1) {
    throw proxy_exception(errno, "set_nonblock");
  }
  ok = rconnect(_socket, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
  if (ok < 0) {
    if (errno == EINPROGRESS) {
      ALBA_LOG(DEBUG, "EINPROGRESS. rpoll");
      struct pollfd pollfd;
      pollfd.fd = _socket;
      pollfd.events = POLLOUT;
      pollfd.revents = 0;
      int nfds = 1;
      int rc = rpoll(&pollfd, nfds,
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         timeout).count());
      if (rc < 0) {
        throw proxy_exception(rc, _build_msg("connect.rpoll"));
      }
      if (rc == 0) {
        throw proxy_exception(rc, "timeout");
      }
    } else {
      throw proxy_exception(errno, "connect");
    }
  }

  _really_write((const char *)(&magic), sizeof(int32_t));
  _really_write((const char *)(&version), sizeof(int32_t));
}

void RDMAProxy_client::_output(llio::message_builder &mb) {
  mb.output_using(_writer);
}

message RDMAProxy_client::_input() {
  message response([&](char *buffer, const int len)
                       -> void { _really_read(buffer, len); });
  return response;
}

void RDMAProxy_client::_expires_from_now(
    const std::chrono::steady_clock::duration &timeout) {
  _deadline = std::chrono::steady_clock::now() + timeout;
  ALBA_LOG(DEBUG, "RDMAProxy_client::_expires_from_now("
                      << std::chrono::duration_cast<std::chrono::milliseconds>(
                             timeout).count() << " ms)");
}

RDMAProxy_client::~RDMAProxy_client() {
  ALBA_LOG(INFO, "~RDMAProxy_client");
  int r = rclose(_socket);
  if (r < 0) {
    int _errno = errno;
    ALBA_LOG(INFO, "exception in close: fd:" << _socket << " r=" << r
                                             << " errno=" << _errno);
  }
}
}
}
