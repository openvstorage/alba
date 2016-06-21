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
#include "osd_info.h"
namespace alba{
namespace proxy_client{

struct asd_slice{
    std::string key;
    uint32_t offset;
    uint32_t len;
    byte* bytes;
};

using namespace proxy_protocol;
class OsdAccess {
public:
    static OsdAccess &getInstance();

    OsdAccess(OsdAccess const &) = delete;
    void operator=(OsdAccess const &) = delete;
    bool osd_is_known(osd_t);

    void update(std::vector<std::pair<osd_t, std::unique_ptr<OsdInfo>>> &infos);

    bool read_osds_slices(std::map<osd_t, std::vector<asd_slice>> &);
private:
    OsdAccess(){}
    std::map<osd_t, std::unique_ptr<OsdInfo>> _osd_infos;

    bool read_osd_slices(osd_t, std::vector<asd_slice>&);
};

}
}
