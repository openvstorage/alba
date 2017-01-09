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

#include "asd_access.h"
#include "rdma_transport.h"
#include "tcp_transport.h"

#include <iostream>

#include <mutex>

namespace alba {
namespace asd {

using alba::proxy_protocol::OsdInfo;

#define LOCK() std::lock_guard<std::mutex> lock(_mutex)

ConnectionPool::ConnectionPool(std::unique_ptr<OsdInfo> config, size_t capacity)
    : config_(std::move(config)), capacity_(capacity) {
  ALBA_LOG(INFO, "Created pool for asd client " << *config_ << ", capacity "
                                                << capacity);
}

ConnectionPool::~ConnectionPool() { clear_(connections_); }

std::unique_ptr<Asd_client> ConnectionPool::pop_(Connections &conns) {
  std::unique_ptr<Asd_client> c;
  if (not conns.empty()) {
    c = std::unique_ptr<Asd_client>(&conns.front());
    conns.pop_front();
  }

  return c;
}

void ConnectionPool::clear_(Connections &conns) {
  while (not conns.empty()) {
    pop_(conns);
  }
}

std::unique_ptr<Asd_client> ConnectionPool::make_one_() const {
  auto duration = std::chrono::seconds(5);

  std::unique_ptr<transport::Transport> transport;
  if (config_->use_rdma) {
    ;
    transport =
        std::unique_ptr<transport::Transport>(new transport::RDMA_transport(
            config_->ips[0], std::to_string(config_->port), duration));
  } else {
    transport =
        std::unique_ptr<transport::Transport>(new transport::TCP_transport(
            config_->ips[0], std::to_string(config_->port), duration));
  }
  std::unique_ptr<Asd_client> c(
      new Asd_client(duration, std::move(transport), config_->long_id));
  return c;
}

void ConnectionPool::release_connection(std::unique_ptr<Asd_client> conn) {
  if (conn) {
    LOCK();
    if (connections_.size() < capacity_) {
      connections_.push_front(*conn.release());
      return;
    }
  }
}

std::unique_ptr<Asd_client>
ConnectionPool::get_connection(ForceNewConnection force_new) {
  std::unique_ptr<Asd_client> conn;

  if (force_new == ForceNewConnection::F) {
    LOCK();
    conn = pop_(connections_);
  }

  if (not conn) {
    conn = make_one_();
  }

  return conn;
}

size_t ConnectionPool::size() const {
  LOCK();
  return connections_.size();
}

size_t ConnectionPool::capacity() const {
  LOCK();
  return capacity_;
}

void ConnectionPool::capacity(size_t cap) {
  Connections tmp;

  {
    LOCK();

    std::swap(capacity_, cap);
    if (connections_.size() > capacity_) {
      for (size_t i = 0; i < capacity_; ++i) {
        Asd_client &c = connections_.front();
        connections_.pop_front();
        tmp.push_front(c);
      }

      std::swap(tmp, connections_);
    }
  }

  clear_(tmp);

  ALBA_LOG(INFO, *config_ << ": updated capacity from " << cap << " to "
                          << capacity());
}

ConnectionPool *
ConnectionPools::get_connection_pool(const proxy_protocol::OsdInfo &osd_info,
                                     int connection_pool_size) {
  LOCK();
  auto it = connection_pools_.find(osd_info.long_id);
  if (it == connection_pools_.end()) {
    ALBA_LOG(INFO, "asd ConnenctionPools adding ConnectionPool for "
                       << osd_info.long_id);
    proxy_protocol::OsdInfo *osd_info_copy =
        new proxy_protocol::OsdInfo(osd_info);

    connection_pools_.emplace(
        osd_info.long_id,
        std::unique_ptr<ConnectionPool>(new ConnectionPool(
            std::unique_ptr<proxy_protocol::OsdInfo>(osd_info_copy),
            connection_pool_size)));
    it = connection_pools_.find(osd_info.long_id);
  }
  return it->second.get();
}
}
}
