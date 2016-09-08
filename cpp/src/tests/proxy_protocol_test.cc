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

#include "gtest/gtest.h"
#include "proxy_protocol.h"
#include "manifest.h"
#include <fstream>

TEST(proxy_protocol, fs3_response) {
  using namespace alba;
  std::ifstream file("./bin/fs3_response.message");
  llio::message m(file);
  using namespace proxy_protocol;
  Status status;
  RoraMap rora_map;
  proxy_protocol::read_write_object_fs3_response(m, status, rora_map);
  // std::cout << "front=" << front << std::endl;
  std::cout << "rora_map=" << rora_map << std::endl;
}
