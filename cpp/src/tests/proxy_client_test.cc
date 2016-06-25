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

#include "gtest/gtest.h"
#include <boost/log/trivial.hpp>
#include <boost/algorithm/string.hpp>
#include "proxy_client.h"
#include "alba_logger.h"
#include "manifest.h"

#include <iostream>
#include <fstream>

using std::string;
using std::cout;
using std::endl;

string env_or_default(const std::string &name, const std::string &def) {
  char *env = getenv(name.c_str());
  if (NULL == env) {
    return def;
  }
  return string(env);
}

auto TIMEOUT = boost::posix_time::seconds(5);
using alba::proxy_client::Proxy_client;
using namespace alba;

struct config_exception : std::exception {
  config_exception(string what) : _what(what) {}
  string _what;

  virtual const char *what() const noexcept { return _what.c_str(); }
};

struct config {
  config() {
    PORT = env_or_default("ALBA_PROXY_PORT", "10000");
    HOST = env_or_default("ALBA_PROXY_IP", "127.0.0.1");
    TRANSPORT = alba::proxy_client::Transport::tcp;
    string transport = env_or_default("ALBA_PROXY_TRANSPORT", "tcp");
    boost::algorithm::to_lower(transport);

    if (transport == "rdma") {
      TRANSPORT = alba::proxy_client::Transport::rdma;
    }
    NAMESPACE = "demo";
  }

  string PORT;
  string HOST;
  string NAMESPACE;
  alba::proxy_client::Transport TRANSPORT;
};

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
    default:
      return &logBoost;
    };
  });
}

TEST(proxy_client, list_objects) {
  init_log();

  ALBA_LOG(WARNING, "starting test:list_objects");
  config cfg;
  cout << "cfg(" << cfg.HOST << ", " << cfg.PORT << ", " << cfg.TRANSPORT << ")"
       << endl;
  auto client = make_proxy_client(cfg.HOST, cfg.PORT, TIMEOUT, cfg.TRANSPORT);
  string ns("demo");
  string first("");

  auto res = client->list_objects(
      ns, first, alba::proxy_client::include_first::T, boost::none,
      alba::proxy_client::include_last::T, -1);

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
  config cfg;
  auto client = make_proxy_client(cfg.HOST, cfg.PORT, TIMEOUT, cfg.TRANSPORT);
  std::string first("");

  auto res = client->list_namespaces(
      first, alba::proxy_client::include_first::T, boost::none,
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
  config cfg;
  auto client = make_proxy_client(cfg.HOST, cfg.PORT, TIMEOUT, cfg.TRANSPORT);

  string name("object name");
  string file("./ocaml/alba.native");

  client->write_object_fs(cfg.NAMESPACE, name, file,
                          proxy_client::allow_overwrite::T, nullptr);

  uint64_t size;
  alba::Checksum *checksum;
  std::tie(size, checksum) = client->get_object_info(
      cfg.NAMESPACE, name, proxy_client::consistent_read::T,
      proxy_client::should_cache::T);

  client->write_object_fs(cfg.NAMESPACE, name, file,
                          proxy_client::allow_overwrite::T, checksum);
  delete checksum;
}

TEST(proxy_client, get_proxy_version) {
  init_log();
  config cfg;
  auto client = make_proxy_client(cfg.HOST, cfg.PORT, TIMEOUT, cfg.TRANSPORT);
  int32_t major;
  int32_t minor;
  int32_t patch;
  std::string hash;

  std::tie(major, minor, patch, hash) = client->get_proxy_version();

  std::cout << "major:" << major << std::endl;
  std::cout << "minor:" << minor << std::endl;
  std::cout << "patch:" << patch << std::endl;
  std::cout << "hash:" << hash << std::endl;
}

double stamp() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  double t0 = tp.tv_sec + (double)tp.tv_usec / 1e6;
  return t0;
}

TEST(proxy_client, test_ping) {
  init_log();
  config cfg;
  auto client = make_proxy_client(cfg.HOST, cfg.PORT, TIMEOUT, cfg.TRANSPORT);
  double eps = 0.05;
  struct timeval timeval0;
  gettimeofday(&timeval0, NULL);
  double t0 = stamp();

  double timestamp = client->ping(1.0);
  double delta = timestamp - t0;
  double t1 = stamp();
  std::cout << "t0:" << t0 << " timestamp:" << timestamp << std::endl;
  std::cout << "delta(t0,timestamp)" << delta << std::endl;
  std::cout << "delta(t1,timestamp)" << t1 - timestamp << std::endl;
  EXPECT_NEAR(delta, 1.0, eps);

  std::cout << "part2" << std::endl;
  t0 = stamp();
  try {
    timestamp = client->ping(10.0);
    // expect failure....
    double t1 = stamp();
    std::cout << "we got here after " << t1 - t0 << " s" << std::endl;
    EXPECT_EQ(true, false);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
    double t1 = stamp();
    delta = t1 - t0;
    std::cout << "t0:" << t0 << " t1:" << t1 << std::endl;
    std::cout << "delta:" << delta << std::endl;
    EXPECT_NEAR(delta, 5.0, eps);
  }
}

void _compare(proxy_protocol::lookup_result_t& exp,
              proxy_protocol::lookup_result_t& act){

    EXPECT_EQ(exp.chunk_index,      act. chunk_index);
    EXPECT_EQ(exp.fragment_index,   act. fragment_index);
    EXPECT_EQ(exp.pos_in_fragment,  act. pos_in_fragment);
    EXPECT_EQ(exp.fragment_length,  act. fragment_length);
    EXPECT_EQ(exp.fragment_version, act. fragment_version);
    EXPECT_EQ(exp._osd,             act. _osd);
}

TEST(proxy_client, manifest){
    using namespace proxy_protocol;
    std::ifstream file;
    file.exceptions ( std::ifstream::failbit | std::ifstream::badbit );
    file.open ("./bin/the_manifest.bin");
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string data = buffer.str();
    auto size = data.size();
    std:: cout <<"size:" << size << std::endl;

    Manifest mf;
    std::vector<char> v(data.begin(), data.end());
    llio::message m(v);
    from(m,mf);

    std::cout << mf << std::endl;
    uint32_t tests[] = {0,
                        10,
                        (1 << 20) + 2,
                        (5 << 20) + 2,
    };

    lookup_result_t expecteds[] ={
        lookup_result_t( 0, 0, 0, 1 << 20 ,0, 9),
        lookup_result_t( 0, 0,10, 1 << 20 ,0, 9),
        lookup_result_t( 0, 1, 2, 1 << 20 ,0, 6),
        lookup_result_t( 1, 0, 2, 1 << 20 ,0, 9),
    };

    for (int i = 0; i< 4; i++){
        auto pos = tests[i];
        auto& expected = expecteds[i];
        auto maybelookup_result = mf.to_chunk_fragment(pos);
        EXPECT_FALSE(maybelookup_result == boost::none);
        auto &lookup_result = *maybelookup_result;
        _compare (expected, lookup_result);
    }

}

TEST(proxy_client, test_osd_info){
    init_log();
    config cfg;
    auto client = make_proxy_client(cfg.HOST, cfg.PORT, TIMEOUT, cfg.TRANSPORT);

    std::vector<std::pair<osd_t,
                          std::unique_ptr<proxy_protocol::OsdInfo>
                          >
               > result;

    client -> osd_info(result);
    for(auto& p: result){
        auto osd = p.first;
        const proxy_protocol::OsdInfo& osd_info = *p.second;
        std::cout << "osd:" << osd <<" info: " << osd_info << std::endl;
    }
}

void _generic_partial_read_test(std::string& namespace_,
                                std::string& name,
                                std::vector<proxy_protocol::ObjectSlices>& objects_slices){
    init_log();
    config cfg;
    auto client = make_proxy_client(cfg.HOST, cfg.PORT, TIMEOUT, cfg.TRANSPORT);
    client -> create_namespace(namespace_, boost::none);

    string file("./ocaml/alba.native");
    client -> write_object_fs(namespace_, name, file,
                              proxy_client::allow_overwrite::T,
                              nullptr);
    client -> read_objects_slices(namespace_,

                                  objects_slices,
                                  proxy_client::consistent_read::F);
}

TEST(proxy_client, test_partial_read_trivial){
    std::string namespace_("test_partial_read_trivial");
    std::ostringstream sos;
    sos << "with_manifest" << std::rand();
    string name = sos.str();
    using namespace proxy_protocol;
    byte * buf = new byte[8192];
    SliceDescriptor sd {buf, 0,4096};

    std::vector<SliceDescriptor> slices { sd };
    ObjectSlices object_slices{name, slices};
    std::vector<ObjectSlices> objects_slices {object_slices};
    _generic_partial_read_test(namespace_, name, objects_slices);
    delete [] buf;
}

TEST(proxy_client, test_partial_read_multislice){
    std::string namespace_("test_partial_read_multi_slice");
    std::ostringstream sos;
    sos << "with_manifest" << std::rand();
    string name = sos.str();
    using namespace proxy_protocol;
    byte * buf = new byte[8192];
    SliceDescriptor sd {buf, 0,4096};

    //slice that spans 2 fragments.
    SliceDescriptor sd2{&buf[4096], (1<<20) -10 , 4096};

    std::vector<SliceDescriptor> slices { sd , sd2 };
    ObjectSlices object_slices{name, slices};
    std::vector<ObjectSlices> objects_slices {object_slices};
    _generic_partial_read_test(namespace_, name, objects_slices);
    delete [] buf;
}

TEST(proxy_client, test_partial_reads){
    init_log();
    config cfg;
    std::string namespace_("test_partial_reads");
    std::ostringstream sos;
    sos << "with_manifest" << std::rand();
    string name = sos.str();
    using namespace proxy_protocol;
    byte * buf = new byte[32768];
    auto client = make_proxy_client(cfg.HOST, cfg.PORT, TIMEOUT, cfg.TRANSPORT);
    client -> create_namespace(namespace_, boost::none);
    string file("./ocaml/alba.native");
    client -> write_object_fs(namespace_, name, file,
                              proxy_client::allow_overwrite::T,
                              nullptr);
    for(int i = 0;i < 8;i++){
        SliceDescriptor sd {buf, 0, 4096};
        std::vector<SliceDescriptor> slices { sd };
        ObjectSlices object_slices{name, slices};
        std::vector<ObjectSlices> objects_slices {object_slices};
        client -> read_objects_slices(namespace_,
                                      objects_slices,
                                      proxy_client::consistent_read::F);
    }
    delete [] buf;
}
