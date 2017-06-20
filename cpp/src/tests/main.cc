/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#include "alba_common.h"
#include "alba_logger.h"

#include <string>

#include <gtest/gtest.h>

#include <boost/log/trivial.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

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
  alba::initialize_libgcrypt();

  std::srand(std::time(0));
  int res = RUN_ALL_TESTS();

  return res;
}
