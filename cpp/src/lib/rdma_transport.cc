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

#include "rdma_transport.h"
#include <boost/lexical_cast.hpp>
#include <rdma/rsocket.h>

namespace alba {
namespace transport {

using std::string;
using llio::message;

RDMA_transport::RDMA_transport(int socket) : _socket(socket) {}

std::string _build_msg(const std::string &prefix) {
  int _errno = errno;
  std::ostringstream ss;
  ss << prefix << " " << _errno;
  return ss.str();
}

void RDMA_transport::write_exact(const char *buf, const int len) {
  if (len == 0) {
    return;
  }
  if (len < 0) {
    throw transport_exception("really_write negative length");
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
    int rc = rpoll(
        &pollfd, nfds,
        std::chrono::duration_cast<std::chrono::milliseconds>(time_remaining)
            .count());
    ALBA_LOG(DEBUG, "rc=" << rc);
    if (rc < 0) {
      throw transport_exception(_build_msg("really_write.rpoll:"));
    }
    if (rc == 0) {
      throw transport_exception("really_write.rpoll timeout");
    }

    sent = rsend(_socket, &buf[off], todo, flags);
    if (sent < 0) {
      throw transport_exception(_build_msg("really_write.send"));
    }
    off += sent;
    todo -= sent;
  } while (
      todo > 0 &&
      (time_remaining = _deadline - std::chrono::steady_clock::now()).count() >
          0);
}

void RDMA_transport::read_exact(char *buf, const int len) {
  if (len == 0) {
    return;
  }
  if (len < 0) {
    throw transport_exception(_build_msg("really_read negative length"));
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
    int rc = rpoll(
        &pollfd, nfds,
        std::chrono::duration_cast<std::chrono::milliseconds>(time_remaining)
            .count());
    ALBA_LOG(DEBUG, "rc=" << rc);
    if (rc < 0) {
      throw transport_exception(_build_msg("really_read.rpoll"));
    }
    if (rc == 0) {
      throw transport_exception("really_read.rpoll timeout");
    }

    read = rrecv(_socket, &buf[off], todo, flags);
    if (read <= 0) {
      throw transport_exception(
          _build_msg("really_read.rrecv= " + std::to_string(read)));
    }
    off += read;
    todo -= read;

  } while (
      todo > 0 &&
      (time_remaining = _deadline - std::chrono::steady_clock::now()).count() >
          0);
}

RDMA_transport
RDMA_transport::make(const string &ip, const string &port,
                     const std::chrono::steady_clock::duration &timeout) {

  ALBA_LOG(INFO, "RDMA_transport(" << ip << ", " << port << ")");

  int socket = rsocket(AF_INET, SOCK_STREAM, 0);

  if (socket < 0) {
    throw transport_exception("socket < 0?");
  }
  struct sockaddr_in serv_addr;

  serv_addr.sin_family = AF_INET;

  int port_i = boost::lexical_cast<int>(port);
  serv_addr.sin_port = htons(port_i);

  int ok = inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr);
  if (ok < 0) {
    throw transport_exception(_build_msg("ip"));
  }

  // make it a non-blocking rsocket
  int retcode;
  retcode = rfcntl(socket, F_GETFL, 0);
  if (retcode == -1 || rfcntl(socket, F_SETFL, retcode | O_NONBLOCK) == -1) {
    throw transport_exception(_build_msg("set_nonblock"));
  }
  ok = rconnect(socket, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
  if (ok < 0) {
    if (errno == EINPROGRESS) {
      ALBA_LOG(DEBUG, "EINPROGRESS. rpoll");
      struct pollfd pollfd;
      pollfd.fd = socket;
      pollfd.events = POLLOUT;
      pollfd.revents = 0;
      int nfds = 1;
      int rc =
          rpoll(&pollfd, nfds,
                std::chrono::duration_cast<std::chrono::milliseconds>(timeout)
                    .count());
      if (rc < 0) {
        throw transport_exception(_build_msg("connect.rpoll"));
      }
      if (rc == 0) {
        throw transport_exception("timeout");
      }
    } else {
      throw transport_exception(_build_msg("connect"));
    }
  }

  return RDMA_transport(socket);
}

void RDMA_transport::expires_from_now(
    const std::chrono::steady_clock::duration &timeout) {
  _deadline = std::chrono::steady_clock::now() + timeout;
  ALBA_LOG(DEBUG,
           "RDMAProxy_client::_expires_from_now("
               << std::chrono::duration_cast<std::chrono::milliseconds>(timeout)
                      .count()
               << " ms)");
}

RDMA_transport::~RDMA_transport() {
  ALBA_LOG(INFO, "~RDMA_transport");
  int r = rclose(_socket);
  if (r < 0) {
    int _errno = errno;
    ALBA_LOG(INFO, "exception in close: fd:" << _socket << " r=" << r
                                             << " errno=" << _errno);
  }
}
}
}
