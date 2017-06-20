/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#include "transport_helper.h"
#include "rdma_transport.h"
#include "tcp_transport.h"

namespace alba {
namespace transport {

std::unique_ptr<Transport>
make_transport(const Kind k, const std::string &ip, const std::string &port,
               const std::chrono::steady_clock::duration &timeout) {
  switch (k) {
  case Kind::tcp:
    return std::make_unique<TCP_transport>(ip, port, timeout);
  case Kind::rdma:
    return std::make_unique<RDMA_transport>(ip, port, timeout);
  default:
    // g++ issues bogus:
    // warning: control reaches end of non-void function [-Wreturn-type]
    throw "unknown alba::transport::Kind";
  }
}
}
}
