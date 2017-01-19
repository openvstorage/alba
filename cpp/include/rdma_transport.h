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

#pragma once
#include "transport.h"

namespace alba {
namespace transport {

class RDMA_transport : public Transport {
public:
  ~RDMA_transport();

  void
  expires_from_now(const std::chrono::steady_clock::duration &timeout) override;

  void write_exact(const char *buf, int len) override;
  void read_exact(char *buf, int len) override;

  RDMA_transport(const std::string &ip, const std::string &port,
                 const std::chrono::steady_clock::duration &timeout);

private:
  int _socket;

  std::chrono::steady_clock::time_point _deadline;
};
}
}
