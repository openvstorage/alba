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

#include <boost/log/trivial.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <string>
#include "alba_logger.h"

namespace po = boost::program_options;

void logBoostMethod(alba::logger::AlbaLogLevel /*level */, std::string &msg) {
  // there should actually be a translation from AlbaLogLevel to some boost
  // log level here, but I'm too lazy for this test client
  BOOST_LOG_TRIVIAL(debug) << msg;
}

std::function<void(alba::logger::AlbaLogLevel, std::string &)> logBoost =
    std::function<void(alba::logger::AlbaLogLevel, std::string &)>(
        logBoostMethod);

std::function<void(alba::logger::AlbaLogLevel, std::string &)> *nulllog =
    nullptr;

void init_log() {
  alba::logger::setLogFunction([&](alba::logger::AlbaLogLevel level) {
    switch (level) {
    default:
      return &logBoost;
    };
  });
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  po::options_description desc("Allowed options");

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

  init_log();
  int res = RUN_ALL_TESTS();

  return res;
}
