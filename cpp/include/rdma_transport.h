/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
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
