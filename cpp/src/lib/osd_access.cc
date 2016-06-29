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

bool OsdAccess::osd_is_unknown(osd_t osd) {
  std::lock_guard<std::mutex> lock(_osd_infos_mutex);
  return (_osd_infos.find(osd) == _osd_infos.end());
}

const std::map<osd_t, info_caps>::iterator OsdAccess::_find_osd(osd_t osd){
  std::lock_guard<std::mutex> lock(_osd_infos_mutex);
  return _osd_infos.find(osd);
}

std::shared_ptr<gobjfs::xio::client_ctx>
OsdAccess::_find_ctx(osd_t osd){
  std::lock_guard<std::mutex> lock(_osd_ctxs_mutex);
  auto it = _osd_ctxs.find(osd);
  if(it == _osd_ctxs.end()){
      return nullptr;
  } else{
      return it -> second;
  }
}

void OsdAccess::_set_ctx(osd_t osd,
                         std::shared_ptr<gobjfs::xio::client_ctx> ctx){
  std::lock_guard<std::mutex> lock(_osd_ctxs_mutex);
  _osd_ctxs[osd] = std::move(ctx);
}

void OsdAccess::update(std::vector<std::pair<osd_t, info_caps>> &infos) {
  ALBA_LOG(DEBUG, "OsdAccess::update");
  std::lock_guard<std::mutex> lock(_osd_infos_mutex);

  _osd_infos.clear();
  for (auto &p : infos) {
    _osd_infos.emplace(p.first, std::move(p.second));
  }
}

bool OsdAccess::read_osds_slices(
    std::map<osd_t, std::vector<asd_slice>> &per_osd) {

  for (auto &item : per_osd) {
    osd_t osd = item.first;
    auto &osd_slices = item.second;
    _read_osd_slices(osd, osd_slices);
  }
  return false;
}


using namespace gobjfs::xio;
int OsdAccess::_read_osd_slices(osd_t osd, std::vector<asd_slice> &slices) {
  ALBA_LOG(DEBUG, "OsdAccess::_read_osd_slices(" << osd << ")");

  auto ctx = _find_ctx(osd);

  if (ctx== nullptr) {
    std::shared_ptr<gobjfs::xio::client_ctx_attr> ctx_attr = ctx_attr_new();

    auto it = _find_osd(osd);
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
    int err = ctx_attr_set_transport(ctx_attr, transport_name,
                                     ip, backdoor_port);
    ALBA_LOG(DEBUG, "set_transport err:" << err);
    if(err != 0){
        throw osd_access_exception(err,"ctx_attr_set_transport");
    }

    ctx = ctx_new(ctx_attr);
    err = ctx_init(ctx);
    ALBA_LOG(DEBUG, "ctx_init err:" << err);
    if(err !=0){
        throw osd_access_exception(err, "ctx_init");
    }
    _set_ctx(osd,ctx);
  }
  size_t n_slices = slices.size();
  std::vector<giocb*> iocb_vec(n_slices);
  std::vector<giocb> giocb_vec(n_slices);
  std::vector<std::string> key_vec(n_slices);

  for(uint i = 0; i < n_slices;i++){
      asd_slice& slice = slices[i];
      giocb& iocb = giocb_vec[i];
      iocb . aio_offset = slice.offset;
      iocb . aio_nbytes = slice.len;
      iocb . aio_buf = slice.bytes;
      iocb_vec[i] = &iocb;
      key_vec[i] = slice.key;
  }

  int ret = aio_readv(ctx, key_vec, iocb_vec);
  if (ret == 0) {
    ret = aio_suspendv(ctx, iocb_vec, nullptr /* timeout */);
  }
  for (auto &elem : iocb_vec) {
      aio_finish(ctx, elem);
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
