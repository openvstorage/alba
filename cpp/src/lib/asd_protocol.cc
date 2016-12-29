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

#include "asd_protocol.h"

namespace alba {
namespace asd_protocol {

using std::string;
using std::vector;

using llio::message_builder;
using llio::message;
using llio::to;
using llio::from;

void make_prologue(message_builder &mb, string long_id) {
  mb.add_raw(_MAGIC.data(), _MAGIC.length());
  to(mb, _VERSION);
  to(mb, boost::optional<string>(long_id));
}

void write_partial_get_request(message_builder &mb, string &key,
                               vector<slice> &slices) {
  to<uint32_t>(mb, 11);
  to(mb, key);
  to<uint32_t>(mb, slices.size());
  for (auto &slice : slices) {
    to<uint32_t>(mb, slice.offset);
    to<uint32_t>(mb, slice.length);
  }
  to<uint32_t>(mb, 1);
}

void read_status(message &m, Status &status) {
  uint32_t rc;
  from(m, rc);

  status.set_rc(rc);
}

void read_partial_get_response(message &m, Status &status, bool &success) {
  read_status(m, status);
  if (!status.is_ok())
    return;

  from(m, success);
}
}
}
