/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once
#include "llio.h"
#include <cinttypes>
#include <string>

namespace alba {

static const uint32_t max_int32 = 2147483647;

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

void initialize_libgcrypt();
}
