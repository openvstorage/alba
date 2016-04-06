#include <boost/asio.hpp>
#include <iostream>
#include "proxy_client.h"
#include <boost/log/trivial.hpp>

void level8(){
  boost::asio::ip::tcp::iostream stream;
  stream.expires_from_now(boost::posix_time::seconds(60));
  stream.connect("127.0.0.1", "10000");
  int32_t magic{1148837403};
  int32_t version{1};
  stream.write((const char *)(&magic), 4);
  stream.write((const char *)(&version), 4);
  stream.flush();
  std::cout << stream.rdbuf();
}

using std::string;

void logBoostMethod(alba::logger::AlbaLogLevel /*level */, std::string &msg) {
  // there should actually be a translation from AlbaLogLevel to some boost
  // log level here, but I'm too lazy for this test client
  BOOST_LOG_TRIVIAL(debug) << msg;
}

std::function<void(alba::logger::AlbaLogLevel, std::string &)>
logBoost = std::function<void(alba::logger::AlbaLogLevel, std::string &)>(logBoostMethod);

using namespace alba::proxy_client;


void level7(){
  alba::logger::setLogFunction(
                               [&](alba::logger::AlbaLogLevel level) {
      return &logBoost; });
  
  ALBA_LOG(WARNING, "cucu")
    
  string host("127.0.0.1");
  string port("10000");
  auto timeout = boost::posix_time::seconds(5);
  auto transport = Transport :: tcp;
  auto client = make_proxy_client(host, port, timeout, transport);
  auto version = client -> get_proxy_version();
    
  int major = std::get<0>(version);
  int minor = std::get<1>(version);
  int patch = std::get<2>(version);
  string commit = std::get<3>(version);
  std::cout << "( " << major << ", " << minor << ", " << patch << ", "
            << commit << ")" << std::endl;
}
int main(int /* argc */, char** /*argv */ ){
  //level8();
  level7();
}
