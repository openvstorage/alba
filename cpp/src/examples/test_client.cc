/*
Copyright 2015 iNuron NV

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
using alba::proxy_client::TCPProxy_client;
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
      "port", po::value<string>()->default_value("10000"),
      "the alba proxy port number")
      ( "hostname", po::value<string>()->default_value("127.0.0.1"),
        "the alba proxy port hostname")
      (
        "command", po::value<string>(),
        "the command you want to execute (should be one of download-object, "
        "upload-object, delete-object, list-objects, show-object, "
        "delete-namespace, create-namespace or list-namespaces")
      ( "namespace", po::value<string>(),
        "the namespace for the relevant operation")
      ( "name", po::value<string>(),
        "the name of the object to download/upload/delete")
      ( "allow-cached", po::value<bool>() -> default_value(false),
        "can we use cached information?")
      ( "file", po::value<string>(), "file to work with for download/upload")
      ( "length", po::value<uint32_t>(), "length for partial object read")
      ( "offset", po::value<uint64_t>()->default_value(0),
        "offset for partial object read");
  po::positional_options_description positionalOptions;
  positionalOptions.add("command", 1);

  po::variables_map vm;

  po::store(po::command_line_parser(argc, argv)
                .options(desc)
                .positional(positionalOptions)
                .run(),
            vm);

  ALBA_LOG(WARNING,"hiere");

  if (vm.count("help")) {
    cout << desc << endl;
    return 1;
  }

  
  po::notify(vm);

  auto command = getRequiredStringArg(vm, "command");
  
  string port = vm["port"].as<string>();
  string hostname = vm["hostname"].as<string>();
  bool consistent_read_b = vm["consistent-read"].as<bool>();
  using namespace alba::proxy_client;
  auto timeout = boost::posix_time::seconds(5);
  
  auto transport = Transport :: tcp;
  
  if (vm.count("transport")){
    string transport_s = vm["transport"].as<string>();
    if (transport_s == "rdma"){
      transport = Transport :: rdma;
    } else{
      assert (transport_s == "tcp");
    }
  };
  
  consistent_read _consistent_read =
      consistent_read_b
      ? consistent_read::T
      : consistent_read::F;
  should_cache _should_cache = should_cache::T;

  if ("download-object" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    string file = getRequiredStringArg(vm, "file");
    auto client = make_proxy_client(hostname, port, timeout, transport);
    client -> read_object_fs(ns, name, file, _consistent_read, _should_cache);
  } else if ("download-object-partial" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    string file = getRequiredStringArg(vm, "file");
    auto offset = vm["offset"].as<uint64_t>();
    auto length = getRequiredArg<uint32_t>(vm, "length");
    auto client = make_proxy_client(hostname, port, timeout, transport);
    auto buf = std::unique_ptr<unsigned char>(new unsigned char[length]);
    alba::proxy_protocol::SliceDescriptor slice{buf.get(), offset, length};
    alba::proxy_protocol::ObjectSlices object_slices{name, {slice}};
    client -> read_objects_slices(ns, {object_slices}, _consistent_read);
    std::ofstream fout(file);
    fout.write((char *)buf.get(), length);
  } else if ("upload-object" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    string file = getRequiredStringArg(vm, "file");
    auto client = make_proxy_client(hostname, port, timeout, transport);
    client -> write_object_fs(ns, name, file,
                              alba::proxy_client::allow_overwrite::T, nullptr);
  } else if ("delete-object" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    string name = getRequiredStringArg(vm, "name");
    auto client = make_proxy_client(hostname, port, timeout, transport);
    client -> delete_object(ns, name, alba::proxy_client::may_not_exist::T);
  } else if ("list-objects" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    auto client = make_proxy_client(hostname, port, timeout, transport);
    string first("");
    auto res = client -> list_objects(
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
    auto client = make_proxy_client(hostname, port, timeout, transport);
    try {
      uint64_t size;
      alba::Checksum *checksum;
      std::tie(size, checksum) = client -> get_object_info(ns, name, _consistent_read, _should_cache);
      cout << "size = " << size << "\n checksum = " << *checksum;
      delete checksum;
    } catch (alba::proxy_client::proxy_exception &e) {
      cout << e.what() << endl;
    }
  } else if ("delete-namespace" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    auto client = make_proxy_client(hostname, port, timeout, transport);
    try {
      client -> delete_namespace(ns);
    } catch (alba::proxy_client::proxy_exception &e) {
      cout << e.what() << endl;
    }
  } else if ("create-namespace" == command) {
    string ns = getRequiredStringArg(vm, "namespace");
    auto client = make_proxy_client(hostname, port, timeout, transport);
    try {
      client -> create_namespace(ns, boost::none);
    } catch (alba::proxy_client::proxy_exception &e) {
      cout << e.what() << endl;
    }
  } else if ("list-namespaces" == command) {
    auto client = make_proxy_client(hostname, port, timeout, transport);
    std::vector<string> namespaces;
    alba::proxy_client::has_more has_more;
    std::tie(namespaces, has_more) = client -> list_namespaces(
        "", alba::proxy_client::include_first::T, boost::none,
        alba::proxy_client::include_last::T, -1);
    cout << namespaces << endl << has_more << endl;
  } else if ("invalidate-cache" == command){
      string ns = getRequiredStringArg(vm,"namespace");
      auto client = make_proxy_client(hostname, port, timeout, transport);
      try{
          client -> invalidate_cache(ns);
      }catch(alba::proxy_client::proxy_exception &e){
          cout << e.what() << endl;
      }
  }
  else {
    cout << "got invalid command name. valid options are: "
         << "download-object, upload-object, delete-object and list-objects"
         << endl;
    return 1;
  }

  return 0;
}
