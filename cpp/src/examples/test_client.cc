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

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/optional/optional_io.hpp>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <thread>

#include "stuff.h"
#include "proxy_client.h"
#include "alba_logger.h"
#include "statistics.h"

using std::string;
using std::cout;
using std::endl;

using namespace alba::stuff;

namespace po = boost::program_options;

template <typename T> T getRequiredArg(po::variables_map map, string arg) {
  if (!map.count(arg)) {
    cout << "specify required argument --" << arg << endl;
    exit(1);
  }
  return map[arg].as<T>();
}

string getRequiredStringArg(po::variables_map map, string arg) {
  return getRequiredArg<string>(map, arg);
}

void logBoostMethod(alba::logger::AlbaLogLevel level, std::string &msg) {
  switch (level) {
  case alba::logger::AlbaLogLevel::DEBUG:
    BOOST_LOG_TRIVIAL(debug) << msg;
    break;
  case alba::logger::AlbaLogLevel::INFO:
    BOOST_LOG_TRIVIAL(info) << msg;
    break;
  case alba::logger::AlbaLogLevel::ERROR:
    BOOST_LOG_TRIVIAL(error) << msg;
    break;
  case alba::logger::AlbaLogLevel::WARNING:
    BOOST_LOG_TRIVIAL(warning) << msg;
    break;
  }
}

std::function<void(alba::logger::AlbaLogLevel, std::string &)> logBoost =
    std::function<void(alba::logger::AlbaLogLevel, std::string &)>(
        logBoostMethod);
std::function<void(alba::logger::AlbaLogLevel, std::string &)> *nulllog =
    nullptr;

void init_log() {
  alba::logger::setLogFunction(
      [&](alba::logger::AlbaLogLevel /*level*/) { return &logBoost; });
}
using namespace alba::proxy_client;

void proxy_get_version(const string &host, const string &port,
                       const std::chrono::steady_clock::duration &timeout,
                       const Transport &transport) {
  auto client = make_proxy_client(host, port, timeout, transport);
  auto version = client->get_proxy_version();

  int major = std::get<0>(version);
  int minor = std::get<1>(version);
  int patch = std::get<2>(version);
  string commit = std::get<3>(version);
  cout << "( " << major << ", " << minor << ", " << patch << ", " << commit
       << ")" << endl;
}

using namespace std::chrono;
enum io_pattern_t { FIXED, RANDOM, STRIDE };

void _bench_one_client(std::unique_ptr<Proxy_client> client,
                       std::shared_ptr<alba::statistics::Statistics> stats_p,
                       const string &namespace_, const string &object_name,
                       const int n, const uint32_t block_size,
                       const uint64_t object_size,
                       const io_pattern_t io_pattern) {

  try {
    alba::statistics::Statistics &stats = *stats_p;
    using namespace alba::proxy_protocol;
    uint64_t range = (object_size / block_size) - 1;

    high_resolution_clock::time_point t0 = high_resolution_clock::now();
    high_resolution_clock::time_point t1;

    for (int i = 0; i < n; i++) {
      std::vector<alba::byte> buffer(block_size);
      uint64_t offset = 0;
      uint64_t block_index = 0;
      switch (io_pattern) {
      case STRIDE: {
        block_index = i % range;
      }; break;
      case RANDOM: {
        uint64_t rand = std::rand();
        block_index = rand % range;
      }; break;
      default: {}; break;
      }
      offset = block_size * block_index;
      SliceDescriptor sd{&buffer[0], offset, block_size};
      std::vector<SliceDescriptor> slices{sd};
      ObjectSlices object_slices{object_name, slices};
      std::vector<ObjectSlices> objects_slices{object_slices};
      stats.new_start();
      client->read_objects_slices(namespace_, objects_slices,
                                  consistent_read::F);

      stats.new_stop();
      t1 = high_resolution_clock::now();
      int dur2 = duration_cast<seconds>(t1 - t0).count();
      int reporting_period = 1;
      if (dur2 > reporting_period) {
        auto time2 = system_clock::to_time_t(t1);
        // struct tm tm;
        char buffer[26];
        ctime_r(&time2, buffer);
        buffer[24] = '\0'; // Removes the newline that is added
        // localtime_r(&time2, &tm);
        cout << buffer << " i:" << i << std::endl;
        t0 = t0 + seconds(reporting_period);
      }
    }
  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
  }
}

void partial_read_benchmark(const string &host, const string &port,
                            const std::chrono::steady_clock::duration &timeout,
                            const Transport &transport,
                            const string &namespace_, const string &file_name,
                            const int n, const int n_clients,
                            const boost::optional<RoraConfig> &rora_config,
                            const bool focus, const uint32_t block_size,
                            const io_pattern_t io_pattern) {

  ALBA_LOG(WARNING, "partial_read_benchmark("
                        << host << ", " << port << ", " << transport
                        << ", rora_config =" << rora_config << ")");

  cout << "block_size=" << block_size;
  std::vector<std::shared_ptr<alba::statistics::Statistics>> stats_v;
  std::vector<std::thread> thread_v;
  int rand = std::rand();
  std::ostringstream sos;
  sos << "object_000" << rand << "_";
  string object_base = sos.str();
  const alba::Checksum *checksum = nullptr;
  const uint64_t object_size = get_file_size(file_name);
  if (focus) {
    auto client_p =
        make_proxy_client(host, port, timeout, transport, rora_config);
    string object_name = object_base;
    client_p->write_object_fs(namespace_, object_name, file_name,
                              allow_overwrite::T, checksum);
    ALBA_LOG(INFO, "uploaded" << file_name << " as " << object_name);
  };
  for (int client_index = 0; client_index < n_clients; client_index++) {
    auto client_p =
        make_proxy_client(host, port, timeout, transport, rora_config);
    string object_name;
    if (focus) {
      object_name = object_base;
    } else {
      object_name = object_base + std::to_string(client_index);
      client_p->write_object_fs(namespace_, object_name, file_name,
                                allow_overwrite::T, checksum);
      ALBA_LOG(INFO, "uploaded" << file_name << " as " << object_name);
    }

    auto stats_p = std::shared_ptr<alba::statistics::Statistics>(
        new alba::statistics::Statistics);
    stats_v.push_back(stats_p);
    std::thread t(_bench_one_client, std::move(client_p), stats_p, namespace_,
                  object_name, n, block_size, object_size, io_pattern);

    thread_v.push_back(std::move(t));
  }
  cout << "launched " << n_clients << " client(s). joining" << std::endl;
  for (auto &thread : thread_v) {
    thread.join();
  }
  cout << "----------------" << std::endl;
  for (auto stats_p : stats_v) {
    stats_p->pretty(cout);
    cout << "----------------" << std::endl;
  }
}

int main(int argc, const char *argv[]) {
  init_log();

  ALBA_LOG(WARNING, "logging initialized")

  // gobjfs directly plugs in boost logging.
  namespace logging = boost::log;
  logging::core::get()->set_filter(logging::trivial::severity >=
                                   logging::trivial::info);

  po::options_description desc("Allowed options");
  desc.add_options()("help", "produce help message")(
      "command", po::value<string>(),
      "the command you want to execute:\n"
      "\tdownload-object, download-object-partial, "
      " upload-object, delete-object, list-objects, "
      " show-object, delete-namespace, create-namespace, "
      " list-namespaces, invalidata-cache, proxy-get-version"
      " partial-read-benchmark")("port",
                                 po::value<string>()->default_value("10000"),
                                 "the alba proxy port number")(
      "host", po::value<string>()->default_value("127.0.0.1"),
      "the alba proxy port hostname")

      ("namespace", po::value<string>(),
       "the namespace for the relevant operation")(
          "name", po::value<string>(),
          "the name of the object to download/upload/delete")(
          "allow-cached", po::value<bool>()->default_value(false),
          "can we use cached information?")(
          "consistent-read", po::value<bool>()->default_value(true),
          "consistent read?")("transport", po::value<string>(),
                              "rdma | tcp (default = tcp)")(
          "file", po::value<string>(), "file to work with for download/upload")(
          "length", po::value<uint32_t>(), "length for partial object read")(
          "offset", po::value<uint64_t>()->default_value(0),
          "offset for partial object read")(
          "benchmark-size", po::value<uint32_t>()->default_value(1000),
          "size of benchmark")("use-rora",
                               po::value<bool>()->default_value(true),
                               "use rora fetcher or not")(
          "n-clients", po::value<uint32_t>()->default_value(1),
          "number of clients")("log-level",
                               po::value<string>()->default_value("info"),
                               "log level to use")(
          "use-null-io", po::value<bool>()->default_value(false),
          "fake results from rora without doing any networking")(
          "block-size", po::value<uint32_t>()->default_value(4096),
          "block size for partial read")(
          "io-pattern", po::value<string>()->default_value("fixed"),
          "pattern for partial read benchmark: fixed | random | stride "
          "(default = fixed)")("focus", po::value<bool>()->default_value(false),
                               "if set, all rora partial reads come from the "
                               "same object, and hit the same ASD");

  po::positional_options_description positionalOptions;
  positionalOptions.add("command", 1);

  po::variables_map vm;

  po::store(po::command_line_parser(argc, argv)
                .options(desc)
                .positional(positionalOptions)
                .run(),
            vm);
  po::notify(vm);

  if (vm.count("help")) {
    cout << desc << endl;
    return 1;
  }

  string command = getRequiredStringArg(vm, "command");
  string port = vm["port"].as<string>();
  string host = vm["host"].as<string>();

  string log_level_s = vm["log-level"].as<string>();
  ALBA_LOG(WARNING, "log-level = " << log_level_s);
  if (log_level_s == "debug") {
    ALBA_LOG(WARNING, "setting log level to " << log_level_s);
    logging::core::get()->set_filter(logging::trivial::severity >=
                                     logging::trivial::debug);
  };
  auto timeout = std::chrono::seconds(5);
  Transport transport(Transport::tcp);

  if (vm.count("transport")) {
    string transport_s = vm["transport"].as<string>();
    if (transport_s == "rdma") {
      transport = Transport::rdma;
    } else {
      assert(transport_s == "tcp");
    }
  };

  bool consistent_read_b = vm["consistent-read"].as<bool>();

  consistent_read _consistent_read =
      consistent_read_b ? consistent_read::T : consistent_read::F;
  should_cache _should_cache = should_cache::T;
  ALBA_LOG(WARNING, "parsed command line options: command = " << command);

  if ("download-object" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    string file = getRequiredStringArg(vm, "file");
    auto client = make_proxy_client(host, port, timeout, transport);
    client->read_object_fs(ns, name, file, _consistent_read, _should_cache);
  } else if ("download-object-partial" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    string file = getRequiredStringArg(vm, "file");
    auto offset = vm["offset"].as<uint64_t>();
    auto length = getRequiredArg<uint32_t>(vm, "length");
    auto client = make_proxy_client(host, port, timeout, transport);
    auto buf = std::unique_ptr<unsigned char>(new unsigned char[length]);
    alba::proxy_protocol::SliceDescriptor slice{buf.get(), offset, length};
    alba::proxy_protocol::ObjectSlices object_slices{name, {slice}};
    client->read_objects_slices(ns, {object_slices}, _consistent_read);
    std::ofstream fout(file);
    fout.write((char *)buf.get(), length);
  } else if ("upload-object" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    string file = getRequiredStringArg(vm, "file");
    auto client = make_proxy_client(host, port, timeout, transport);
    client->write_object_fs(ns, name, file,
                            alba::proxy_client::allow_overwrite::T, nullptr);
  } else if ("delete-object" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    auto client = make_proxy_client(host, port, timeout, transport);
    client->delete_object(ns, name, alba::proxy_client::may_not_exist::T);
  } else if ("list-objects" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    auto client = make_proxy_client(host, port, timeout, transport);
    string first("");
    auto res = client->list_objects(
        ns, first, alba::proxy_client::include_first::T, boost::none,
        alba::proxy_client::include_last::T, -1);
    auto objects = std::get<0>(res);
    auto has_more = std::get<1>(res);
    cout << "received " << objects.size() << " objects" << endl;
    cout << "[ ";
    for (auto &object : objects) {
      cout << object << ",\n";
    }
    cout << " ]" << endl;
    cout << has_more << endl;
  } else if ("show-object" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    auto client = make_proxy_client(host, port, timeout, transport);
    try {
      uint64_t size;
      alba::Checksum *checksum;
      std::tie(size, checksum) =
          client->get_object_info(ns, name, _consistent_read, _should_cache);
      cout << "size = " << size << "\n checksum = " << *checksum;
      delete checksum;
    } catch (alba::proxy_client::proxy_exception &e) {
      cout << e.what() << endl;
    }
  } else if ("delete-namespace" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    auto client = make_proxy_client(host, port, timeout, transport);
    try {
      client->delete_namespace(ns);
    } catch (alba::proxy_client::proxy_exception &e) {
      cout << e.what() << endl;
    }
  } else if ("create-namespace" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    auto client = make_proxy_client(host, port, timeout, transport);
    try {
      client->create_namespace(ns, boost::none);
    } catch (alba::proxy_client::proxy_exception &e) {
      cout << e.what() << endl;
    }
  } else if ("list-namespaces" == command) {
    auto client = make_proxy_client(host, port, timeout, transport);
    std::vector<string> namespaces;
    alba::proxy_client::has_more has_more;
    std::tie(namespaces, has_more) = client->list_namespaces(
        "", alba::proxy_client::include_first::T, boost::none,
        alba::proxy_client::include_last::T, -1);

    cout << namespaces << endl
         << has_more << endl;

  } else if ("invalidate-cache" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    auto client = make_proxy_client(host, port, timeout, transport);
    try {
      client->invalidate_cache(ns);
    } catch (alba::proxy_client::proxy_exception &e) {
      cout << e.what() << endl;
    }
  } else if ("proxy-get-version" == command) {
    proxy_get_version(host, port, timeout, transport);

  } else if ("namespace-exists" == command) {
    auto client = make_proxy_client(host, port, timeout, transport);
    string ns = getRequiredStringArg(vm, "namespace");
    bool result = client->namespace_exists(ns);
    cout << "namespace_exists(" << ns << ") => " << result << endl;
  } else if ("partial-read-benchmark" == command) {
    std::map<std::string, io_pattern_t> string_to_pattern;
    string_to_pattern["fixed"] = FIXED;
    string_to_pattern["random"] = RANDOM;
    string_to_pattern["stride"] = STRIDE;
    string ns = getRequiredStringArg(vm, "namespace");
    string file = getRequiredStringArg(vm, "file");
    uint32_t n = getRequiredArg<uint32_t>(vm, "benchmark-size");
    uint32_t n_clients = getRequiredArg<uint32_t>(vm, "n-clients");
    bool use_rora = getRequiredArg<bool>(vm, "use-rora");
    boost::optional<RoraConfig> rora_config = boost::none;

    uint32_t block_size = getRequiredArg<uint32_t>(vm, "block-size");
    bool focus = getRequiredArg<bool>(vm, "focus");
    string io_pattern_s = getRequiredStringArg(vm, "io-pattern");
    io_pattern_t io_pattern = FIXED;
    const auto &it = string_to_pattern.find(io_pattern_s);
    if (it != string_to_pattern.cend()) {
      io_pattern = it->second;
    }

    if (use_rora) {
      bool use_null_io = getRequiredArg<bool>(vm, "use-null-io");
      rora_config = RoraConfig(100, use_null_io);
    }
    partial_read_benchmark(host, port, timeout, transport, ns, file, n,
                           n_clients, rora_config, focus, block_size,
                           io_pattern);
  } else {
    cout << "got invalid command name. valid options are: "
         << "download-object, upload-object, delete-object, list-objects "
         << "show-object, delete-namespace, create-namespace, list-namespaces, "
         << "invalidate-cache, proxy-get-version, namespace-exists" << endl;
    return 1;
  }
  return 0;
}
