/*
  Copyright (C) 2016 iNuron NV

  This file is part of Open vStorage Open Source Edition (OSE), as available
  from


  http://www.openvstorage.org and
  http://www.openvstorage.com.

  This file is free software; you can redistribute it and/or modify it
  under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
  as published by the Free Software Foundation, in version 3 as it comes
  in the <LICENSE.txt> file of the Open vStorage OSE distribution.

  Open vStorage is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY of any kind.
*/
#include "osd_access.h"
#include "alba_logger.h"

#include <assert.h>
#include "stuff.h"

namespace alba {
namespace proxy_client {

OsdAccess &OsdAccess::getInstance() {
  static OsdAccess instance;
  return instance;
}
// TODO: protect resources from // access.

bool OsdAccess::osd_is_unknown(osd_t osd) {
  return (_osd_infos.find(osd) == _osd_infos.end());
}

void OsdAccess::update(std::vector<std::pair<osd_t, info_caps>> &infos) {
  ALBA_LOG(DEBUG, "OsdAccess::update");

  _osd_infos_mutex.lock();

  _osd_infos.clear();
  for (auto &p : infos) {
    _osd_infos.emplace(p.first, std::move(p.second));
  }

  _osd_infos_mutex.unlock();
}

bool OsdAccess::read_osds_slices(
    std::map<osd_t, std::vector<asd_slice>> &per_osd) {
  for (auto &item : per_osd) {
    osd_t osd = item.first;
    auto &osd_slices = item.second;
    read_osd_slices(osd, osd_slices);
  }
  return false;
}

using namespace gobjfs::xio;
int OsdAccess::read_osd_slices(osd_t osd, std::vector<asd_slice> &slices) {
  ALBA_LOG(DEBUG, "OsdAccess::read_osd_slices(" << osd << ")");

  auto ctx_it = _osd_ctxs.find(osd);

  if (ctx_it == _osd_ctxs.end()) {
    std::shared_ptr<gobjfs::xio::client_ctx_attr> ctx_attr = ctx_attr_new();

    auto it = _osd_infos.find(osd);
    const auto &ic = it->second;
    const auto &osd_info = ic.first;
    const auto &osd_caps = ic.second;
    std::string transport_name("tcp");
    if (osd_info->use_rdma) {
      transport_name = "rdma";
    }
    if (boost::none == osd_caps->port) {
      return 1;
    }

    int backdoor_port = *osd_caps->port;

    std::string &ip = osd_info->ips[0];
    ALBA_LOG(DEBUG, "osd:" << osd << " ip:" << ip
                           << " port: " << backdoor_port);
    int err = ctx_attr_set_transport(ctx_attr, transport_name.c_str(),
                                     ip.c_str(), backdoor_port);
    ALBA_LOG(DEBUG, "set_transport err:" << err);
    assert(err == 0);
    std::shared_ptr<gobjfs::xio::client_ctx> ctx = ctx_new(ctx_attr);
    err = ctx_init(ctx);
    ALBA_LOG(DEBUG, "ctx_init err:" << err);
    assert(err == 0);
    _osd_ctxs.emplace(osd, ctx);
    ctx_it = _osd_ctxs.find(osd);
  }
  auto ctx = ctx_it->second;

  std::vector<giocb *> iocb_vec;
  std::vector<std::string> key_vec;
  for (auto &slice : slices) {

    giocb *iocb = (giocb *)malloc(sizeof(giocb));
    iocb->aio_offset = slice.offset;
    iocb->aio_nbytes = slice.len;
    iocb->aio_buf = slice.bytes;
    std::string &key = slice.key;
    iocb_vec.push_back(iocb);
    key_vec.push_back(key);
  }
  int ret = aio_readv(ctx, key_vec, iocb_vec);
  if (ret == 0) {
    ret = aio_suspendv(ctx, iocb_vec, nullptr /* timeout */);
  }
  for (auto &elem : iocb_vec) {
    aio_finish(ctx, elem);
    free(elem);
  }
  ALBA_LOG(DEBUG, "osd_access: ret=" << ret);

  // TODO: when is the context considered 'bad' (and should be removed from the
  // map) ?

  // ctx_destroy(ctx);
  // ctx_attr_destroy(ctx_attr);
  return ret;
}
}
}
