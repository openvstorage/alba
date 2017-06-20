/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include "alba_logger.h"
#include "llio.h"

#include <chrono>

#include <boost/asio.hpp>

namespace alba {
namespace transport {

enum class Kind { tcp, rdma };
std::ostream &operator<<(std::ostream &, Kind);
std::istream &operator>>(std::istream &, Kind &);

struct transport_exception : std::exception {
  transport_exception(std::string what) : _what(what) {}

  std::string _what;

  virtual const char *what() const noexcept { return _what.c_str(); }
};

class Transport {
public:
  virtual void
  expires_from_now(const std::chrono::steady_clock::duration &timeout) = 0;

  virtual void write_exact(const char *buf, int len) = 0;
  virtual void read_exact(char *buf, int len) = 0;

  virtual ~Transport(){};

  llio::message read_message();
  void output(llio::message_builder &);
};
}
}
