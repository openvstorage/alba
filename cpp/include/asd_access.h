// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes in
// the LICENSE.txt file of the Open vStorage OSE distribution.
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.

#pragma once

#include <iosfwd>
#include <memory>

#include <boost/intrusive/slist.hpp>

#include "boolean_enum.h"
#include "spinlock.h"

#include "asd_client.h"
#include "osd_info.h"

BOOLEAN_ENUM(ForceNewConnection)

namespace alba {
namespace asd {

using asd_client::Asd_client;

class ConnectionPool {
public:
  static std::shared_ptr<ConnectionPool>
  create(std::unique_ptr<proxy_protocol::OsdInfo>, size_t capacity);

  ConnectionPool(std::unique_ptr<proxy_protocol::OsdInfo>, size_t);

  ~ConnectionPool();

  ConnectionPool(const ConnectionPool &) = delete;

  ConnectionPool &operator=(const ConnectionPool &) = delete;

  std::unique_ptr<Asd_client>
  get_connection(const ForceNewConnection = ForceNewConnection::F);

  void capacity(const size_t);

  size_t capacity() const;

  size_t size() const;

private:
  mutable alba::spinlock::SpinLock lock_;

  using Connections = boost::intrusive::slist<Asd_client>;
  Connections connections_;

  std::unique_ptr<proxy_protocol::OsdInfo> config_;
  size_t capacity_;

  std::unique_ptr<Asd_client> make_one_() const;

  static std::unique_ptr<Asd_client> pop_(Connections &);

  static void clear_(Connections &);

  void release_connection_(Asd_client *);
};

class ConnectionPools {
public:
  ConnectionPool *get_connection_pool(const proxy_protocol::OsdInfo &);

  ~ConnectionPools();

  ConnectionPools(const ConnectionPools &) = delete;

  ConnectionPools &operator=(const ConnectionPools &) = delete;

private:
  mutable alba::spinlock::SpinLock lock_;
  std::map<std::string, ConnectionPool *> connection_pools_;
};
}
}
