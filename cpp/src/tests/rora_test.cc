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
#include <iostream>
#include <fstream>

#include "manifest.h"
#include "proxy_protocol.h"
#include "helper.h"
#include "manifest_cache.h"
#include "partial_read_planner.h"
#include "osd_access.h"

using namespace alba;

TEST(rora, calculation) {
  using namespace proxy_protocol;
  std::string names[] = {
      "./bin/fs3_response.message",
      //"./bin/fs3_response2.message",
  };
  for (auto file_name : names) {
    std::ifstream file(file_name);
    llio::message m(file);
    Status status;
    RoraMap rora_map;
    read_write_object_fs3_response(m, status, rora_map);

    std::string oid = "object_id";
    std::string aid = "alba_id";

    int lengths[] = {4096, 16000, 400000, 2000000};
    int n_tests = 1;
    for (int i = 0; i < n_tests; i++) {
      uint32_t slice_size = lengths[i];
      uint32_t offset = 5 << 20; // chunk 1;
      SliceDescriptor sd{nullptr, offset, slice_size};

      std::vector<SliceDescriptor> sds{sd};
      //std::map<osd_t, std::vector<proxy_client::asd_slice>> per_osd;
      std::vector<std::shared_ptr<proxy_client::Instruction>> instructions;

      for(auto& sd : sds){
          proxy_protocol::calculate_instructions_for_slice(rora_map, sd, instructions);
      }

      using stuff::operator<<;
      for(auto& i: instructions){

          std::cout << "instruction: ";
          std::cout << *i;
          //i -> pretty(std::cout);
          std::cout << std::endl;
      }
    }
  }
}
