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
#include "generic_proxy_client.h"

namespace alba {
namespace proxy_client {

class RDMAProxy_client : public GenericProxy_client {
public:
  RDMAProxy_client(const std::string &ip, const std::string &port,
                   const boost::asio::time_traits<
                       boost::posix_time::ptime>::duration_type &expiry_time);

  ~RDMAProxy_client();

private:
  int _socket;
  void _really_write(const char *buf, int len);
  void _really_read(char *buf, int len);
  void check_status(const char *function_name);

  std::function<void(const char *, const int)> _writer;
  std::function<void(char *, const int)> _reader;

  void _expires_from_now(const boost::asio::time_traits<
      boost::posix_time::ptime>::duration_type &expiry_time);
  int _request_time_left; /* ms */
  void _output(llio::message_builder &mb);
  llio::message _input();
};
}
}
