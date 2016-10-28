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
#include "llio.h"
#include <cinttypes>
#include <string>

namespace alba {

struct x_uint64_t {
  uint64_t i;
};

std::ostream &operator<<(std::ostream &, const x_uint64_t &);
void to_be(alba::llio::message_builder &mb, const x_uint64_t &t);
bool operator<(const x_uint64_t &lhs, const x_uint64_t &rhs);

typedef x_uint64_t osd_t;
typedef x_uint64_t namespace_t;
typedef unsigned char byte;
typedef std::string alba_id_t;
}
