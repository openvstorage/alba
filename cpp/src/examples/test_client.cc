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

#include <fstream>

#include "stuff.h"
#include "proxy_client.h"
#include "alba_logger.h"

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

using namespace alba::proxy_client;

void proxy_get_version(const string &host, const string &port,
                       const boost::asio::time_traits<
                           boost::posix_time::ptime>::duration_type &timeout,
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

int main(int argc, const char *argv[]) {
  alba::logger::setLogFunction([&](alba::logger::AlbaLogLevel level) {
    switch (level) {
    case alba::logger::AlbaLogLevel::WARNING:
      return &logBoost;
    default:
      return nulllog;
    };
  });

  ALBA_LOG(WARNING, "cucu")

  po::options_description desc("Allowed options");
  desc.add_options()("help", "produce help message")(
      "command", po::value<string>(),
      "the command you want to execute:\n"
      "\tdownload-object, download-object-partial, "
      " upload-object, delete-object, list-objects, "
      " show-object, delete-namespace, create-namespace, "
      " list-namespaces, invalidata-cache, proxy-get-version")(
      "port", po::value<string>()->default_value("10000"),
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
          "offset for partial object read");
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

  auto timeout = boost::posix_time::seconds(5);
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
  } else {
    cout << "got invalid command name. valid options are: "
         << "download-object, upload-object, delete-object, list-objects "
         << "show-object, delete-namespace, create-namespace, list-namespaces, "
         << "invalidate-cache, proxy-get-version, namespace-exists" << endl;
    return 1;
  }
  return 0;
}
