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

#include <gtest/gtest.h>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

namespace po = boost::program_options;

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  po::options_description desc("Allowed options");
  ;
  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

  int res = RUN_ALL_TESTS();

  return res;
}
