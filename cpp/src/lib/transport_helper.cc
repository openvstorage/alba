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
    return alba::stuff::make_unique<TCP_transport>(ip, port, timeout);
  case Kind::rdma:
    return alba::stuff::make_unique<RDMA_transport>(ip, port, timeout);
  default:
    // g++ issues bogus:
    // warning: control reaches end of non-void function [-Wreturn-type]
    throw "unknown alba::transport::Kind";
  }
}
}
}
