/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include "asd_protocol.h"
#include "transport.h"

#include <boost/asio.hpp>
#include <boost/intrusive/slist.hpp>
#include <chrono>
#include <tuple>
#include <vector>

namespace alba {
namespace asd_client {

using std::string;
using std::vector;
using asd_protocol::slice;

struct asd_exception : std::exception {
  asd_exception(uint32_t return_code, std::string what)
      : _return_code(return_code), _what(what) {}

  uint32_t _return_code;
  std::string _what;

  virtual const char *what() const noexcept { return _what.c_str(); }
};

class Asd_client : public boost::intrusive::slist_base_hook<> {
public:
  Asd_client(const std::chrono::steady_clock::duration &,
             std::unique_ptr<transport::Transport> &&,
             boost::optional<string> long_id);

  void partial_get(string &, vector<slice> &);
  void set_slowness(asd_protocol::slowness_t &slowness);
  std::tuple<int32_t, int32_t, int32_t, std::string> get_version();

private:
  void init_(boost::optional<string> long_id);

  asd_protocol::Status _status;
  std::unique_ptr<transport::Transport> _transport;
  const std::chrono::steady_clock::duration _timeout;
  llio::message_builder _mb;
  void check_status(const char *function_name);
};
}
}
