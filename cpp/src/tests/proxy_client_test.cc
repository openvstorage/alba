/*
Copyright 2015 Open vStorage NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "gtest/gtest.h"
#include <boost/log/trivial.hpp>
#include "proxy_client.h"
#include "alba_logger.h"

using std::string;
using std::cout;
using std::endl;

const string PORT("10000");
const string HOSTNAME("127.0.0.1");
const string NAMESPACE("demo");
auto TIMEOUT = boost::posix_time::seconds(5);
using alba::proxy_client::Proxy_client;
using namespace alba;

void logBoostMethod(alba::logger::AlbaLogLevel /*level */, string &msg) {
  // there should actually be a translation from AlbaLogLevel to some boost
  // log level here, but I'm too lazy for this test client
  BOOST_LOG_TRIVIAL(debug) << msg;
}

std::function<void(alba::logger::AlbaLogLevel, string &)> logBoost =
    std::function<void(alba::logger::AlbaLogLevel, string &)>(logBoostMethod);

std::function<void(alba::logger::AlbaLogLevel, string &)> *nulllog = nullptr;

void init_log() {
  alba::logger::setLogFunction([&](alba::logger::AlbaLogLevel level) {
    switch (level) {
    case alba::logger::AlbaLogLevel::WARNING:
      return &logBoost;
    default:
      return nulllog;
    };
  });
}

TEST(proxy_client, list_objects) {
  init_log();
  ALBA_LOG(WARNING, "starting test:list_objects");
  Proxy_client client(HOSTNAME, PORT, TIMEOUT);
  string ns("demo");
  string first("");

  auto res =
      client.list_objects(ns, first, alba::proxy_client::include_first::T,
                          boost::none, alba::proxy_client::include_last::T, -1);

  auto objects = std::get<0>(res);
  auto has_more = std::get<1>(res);
  cout << "received" << objects.size() << " objects" << endl;
  cout << "[ ";
  for (auto &object : objects) {
    cout << object << ",\n";
  }
  cout << " ]" << endl;
  cout << has_more << endl;
  cout << "size ok?" << endl;
  EXPECT_EQ(0, objects.size());
  cout << "has_more ok?" << endl;
  EXPECT_EQ(false, BooleanEnumTrue(has_more));
  cout << "end of test" << endl;
}

TEST(proxy_client, list_namespaces) {
  init_log();
  ALBA_LOG(WARNING, "starting test:list_namespaces");
  Proxy_client client(HOSTNAME, PORT, TIMEOUT);
  std::string first("");

  auto res = client.list_namespaces(first, alba::proxy_client::include_first::T,
                                    boost::none,
                                    alba::proxy_client::include_last::T, -1);

  auto objects = std::get<0>(res);
  alba::proxy_client::has_more has_more = std::get<1>(res);
  cout << "received" << objects.size() << " objects" << endl;
  cout << "[ ";
  for (auto &object : objects) {
    cout << object << ",\n";
  }
  cout << " ]" << endl;
  cout << has_more << endl;
  EXPECT_EQ(1, objects.size());
  EXPECT_EQ(false, BooleanEnumTrue(has_more));
}

TEST(proxy_client, get_object_info) {
  init_log();
  Proxy_client client(HOSTNAME, PORT, TIMEOUT);

  string name("object name");
  string file("./proxy.cfg");

  client.write_object_fs(NAMESPACE, name, file,
                         proxy_client::allow_overwrite::T, nullptr);

  uint64_t size;
  alba::Checksum *checksum;
  std::tie(size, checksum) =
      client.get_object_info(NAMESPACE, name, proxy_client::consistent_read::T,
                             proxy_client::should_cache::T);

  client.write_object_fs(NAMESPACE, name, file,
                         proxy_client::allow_overwrite::T, checksum);
  delete checksum;
}

TEST(proxy_client, get_proxy_version) {
  init_log();
  Proxy_client client(HOSTNAME, PORT, TIMEOUT);
  int32_t major;
  int32_t minor;
  int32_t patch;
  std::string hash;

  std::tie(major, minor, patch, hash) = client.get_proxy_version();

  std::cout << "major:" << major << std::endl;
  std::cout << "minor:" << minor << std::endl;
  std::cout << "patch:" << patch << std::endl;
  std::cout << "hash:" << hash << std::endl;
}
