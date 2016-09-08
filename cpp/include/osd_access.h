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
#include "alba_common.h"
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include "osd_info.h"
#include <gobjfs_client.h>
namespace alba {
namespace proxy_client {

struct asd_slice {
  std::string key;
  uint32_t offset;
  uint32_t len;
  byte *bytes;
};

struct osd_access_exception : std::exception {
  osd_access_exception(uint32_t return_code, std::string what)
      : _return_code(return_code), _what(what) {}

  uint32_t _return_code;
  std::string _what;

  virtual const char *what() const noexcept { return _what.c_str(); }
};

using namespace proxy_protocol;


class OsdAccess {
public:
  static OsdAccess &getInstance();

  OsdAccess(OsdAccess const &) = delete;
  void operator=(OsdAccess const &) = delete;
  bool osd_is_unknown(const alba_id_t&, osd_t);

  void update(rora_osd_map_t& info_map);

  int read_osds_slices(alba_id_t&, std::map<osd_t, std::vector<asd_slice>> &);

private:
  OsdAccess() {}
  std::mutex _alba_map_mutex;
  rora_osd_map_t _alba_map;

  std::mutex _osd_ctxs_mutex;
  std::map<alba_id_t, std::map<osd_t, std::shared_ptr<gobjfs::xio::client_ctx>>> _osd_ctxs;

  std::shared_ptr<info_caps>  _find_osd(alba_id_t&, osd_t);

  int _read_osd_slices(alba_id_t&, osd_t, std::vector<asd_slice> &);
  std::shared_ptr<gobjfs::xio::client_ctx> _find_ctx(alba_id_t&, osd_t);
  void _set_ctx(alba_id_t&, osd_t, std::shared_ptr<gobjfs::xio::client_ctx>);
  void _remove_ctx(alba_id_t&, osd_t);
};

std::ostream &operator<<(std::ostream &, const asd_slice &);
}
}
