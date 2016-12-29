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

#pragma once

#include "asd_protocol.h"
#include "transport.h"

#include <boost/asio.hpp>
#include <chrono>
#include <vector>

namespace alba {
namespace asd_client {

using std::string;
using std::vector;
using asd_protocol::slice;

struct asd_exception : std::exception {
  asd_exception(std::string what) : _what(what) {}

  std::string _what;

  virtual const char *what() const noexcept { return _what.c_str(); }
};

class Asd_client {
public:
  static Asd_client make_client(std::unique_ptr<transport::Transport> &&,
                                boost::optional<string> long_id,
                                const std::chrono::steady_clock::duration &);

  void partial_get(string &, vector<slice> &);

private:
  Asd_client(const std::chrono::steady_clock::duration &,
             std::unique_ptr<transport::Transport> &&);

  asd_protocol::Status _status;
  std::unique_ptr<transport::Transport> _transport;
  const std::chrono::steady_clock::duration _timeout;
};
}
}
