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

const string _MAGIC = "aLbA";
const uint32_t _VERSION = 1;

void make_prologue(message_builder &mb, boost::optional<string> long_id) {
  mb.add_raw(_MAGIC.data(), _MAGIC.length());
  to<uint32_t>(mb, _VERSION);
  to(mb, long_id);
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

void write_set_slowness_request(message_builder &mb, const slowness_t& slowness){
  to<uint32_t>(mb, SLOWNESS);
  //to<slowness_t>(mb, slowness);
  if (boost::none == slowness){
      to(mb, false);
  }else{
      to(mb, true);
      to(mb, *slowness);
  }


}


void read_set_slowness_response(message &m, Status& status){
  read_status(m, status);
}


void write_get_version_request(message_builder &mb) {
    to<uint32_t>(mb, GET_VERSION);
}

void read_get_version_response(message &m, Status &status, int32_t &major,
                                     int32_t &minor, int32_t &patch,
                                     std::string &hash) {
  read_status(m, status);
  if (status.is_ok()) {
      from(m, major);
      from(m, minor);
      from(m, patch);
      from(m, hash);
  }
}

}
}
