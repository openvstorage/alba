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

#include "proxy_client.h"
#include "tcp_proxy_client.h"
#include "rdma_proxy_client.h"

#include "rora_proxy_client.h"

#include "alba_logger.h"

#include <iostream>

#include <errno.h>
#include <boost/lexical_cast.hpp>

namespace alba {
namespace proxy_client {

std::unique_ptr<Proxy_client> _make_proxy_client(
    const std::string &ip, const std::string &port,
    const boost::asio::time_traits<boost::posix_time::ptime>::duration_type &
        expiry_time,
    const Transport &transport) {
  Proxy_client *r = nullptr;

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

std::unique_ptr<Proxy_client> make_proxy_client(
    const std::string &ip, const std::string &port,
    const boost::asio::time_traits<boost::posix_time::ptime>::duration_type &
        expiry_time,
    const Transport &transport) {

  return std::unique_ptr<Proxy_client>(new RoraProxy_client(
      _make_proxy_client(ip, port, expiry_time, transport)));
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
