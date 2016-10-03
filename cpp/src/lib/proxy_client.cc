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
#include "rdma_proxy_client.h"
#include "tcp_proxy_client.h"

#include "rora_proxy_client.h"

#include "alba_logger.h"

#include <iostream>

#include <boost/lexical_cast.hpp>
#include <errno.h>

namespace alba {
namespace proxy_client {

std::unique_ptr<GenericProxy_client>
_make_proxy_client(const std::string &ip, const std::string &port,
                   const std::chrono::steady_clock::duration &timeout,
                   const Transport &transport) {
  GenericProxy_client *r = nullptr;

  switch (transport) {
  case Transport::tcp: {
    r = new TCPProxy_client(ip, port, timeout);
  }; break;
  case Transport::rdma: {
    r = new RDMAProxy_client(ip, port, timeout);
  }; break;
  }
  std::unique_ptr<GenericProxy_client> result(r);
  return result;
}

std::unique_ptr<Proxy_client>
make_proxy_client(const std::string &ip, const std::string &port,
                  const std::chrono::steady_clock::duration &timeout,
                  const Transport &transport,
                  const boost::optional<RoraConfig> &rora_config) {

  std::unique_ptr<GenericProxy_client> inner_client =
      _make_proxy_client(ip, port, timeout, transport);

  if (boost::none == rora_config) {
    // work around g++ 4.[8|9] bug:
    return std::unique_ptr<Proxy_client>(inner_client.release());
  } else {
    return std::unique_ptr<Proxy_client>(
        new RoraProxy_client(std::move(inner_client), *rora_config));
  }
}

void Proxy_client::apply_sequence(
    const std::string &namespace_, const write_barrier write_barrier,
    const sequences::Sequence &seq,
    std::vector<proxy_protocol::object_info> &object_infos) {
  this->apply_sequence(namespace_, write_barrier, seq._asserts, seq._updates,
                       object_infos);
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

std::ostream &operator<<(std::ostream &os, const RoraConfig &cfg) {
  os << "RoraConfig{"
     << "manifest_cache_size=" << cfg.manifest_cache_size << "}";
  return os;
}
}
}
