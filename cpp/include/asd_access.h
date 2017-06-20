/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include <iosfwd>
#include <memory>

#include <boost/intrusive/slist.hpp>

#include "boolean_enum.h"
#include <mutex>

#include "asd_client.h"
#include "osd_info.h"

namespace alba {
namespace asd {

using namespace std::chrono;
using asd_client::Asd_client;

class ConnectionPool {
public:
  ConnectionPool(std::unique_ptr<proxy_protocol::OsdInfo>, size_t,
                 std::chrono::steady_clock::duration timeout);

  ~ConnectionPool();

  ConnectionPool(const ConnectionPool &) = delete;

  ConnectionPool &operator=(const ConnectionPool &) = delete;

  std::unique_ptr<Asd_client> get_connection();

  void capacity(const size_t);

  size_t capacity() const;

  size_t size() const;

  void release_connection(std::unique_ptr<Asd_client>);
  void report_failure();

private:
  mutable std::mutex _mutex;

  using Connections = boost::intrusive::slist<Asd_client>;
  Connections connections_;

  std::unique_ptr<proxy_protocol::OsdInfo> config_;
  size_t capacity_;

  std::chrono::steady_clock::duration timeout_;

  std::unique_ptr<Asd_client> make_one_() const;

  static std::unique_ptr<Asd_client> pop_(Connections &);

  static void clear_(Connections &);

  int _fast_path_failures;
  steady_clock::time_point _failure_time;
};

class ConnectionPools {
public:
  ConnectionPool *
  get_connection_pool(const proxy_protocol::OsdInfo &, int connection_pool_size,
                      std::chrono::steady_clock::duration timeout);

  ConnectionPools() = default;

  ConnectionPools(const ConnectionPools &) = delete;

  ConnectionPools &operator=(const ConnectionPools &) = delete;

private:
  mutable std::mutex _mutex;
  std::map<std::string, std::unique_ptr<ConnectionPool>> connection_pools_;
};
}
}
