/*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
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
