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

#include "alba_common.h"
#include <gcrypt.h>
#include <iostream>

namespace alba {

namespace llio {

template <> void from(message &m, x_uint64_t &t) {
  uint32_t i32;
  from(m, i32);
  if (i32 < max_int32) {
    t.i = i32;
  } else {
    from(m, t.i);
  }
}
}

void to_be(alba::llio::message_builder &mb, const x_uint64_t &t) {
  if (t.i < max_int32) {
    alba::llio::to_be(mb, (uint32_t)t.i);
  } else {
    alba::llio::to_be(mb, (uint32_t)max_int32);
    alba::llio::to_be(mb, t.i);
  }
}

bool operator<(const x_uint64_t &lhs, const x_uint64_t &rhs) {
  return lhs.i < rhs.i;
}

std::ostream &operator<<(std::ostream &os, const x_uint64_t &t) {
  os << t.i;
  return os;
}

void initialize_libgcrypt() {
  /* Version check should be the very first call because it
     makes sure that important subsystems are initialized. */
  if (!gcry_check_version(GCRYPT_VERSION)) {
    fputs("libgcrypt version mismatch\n", stderr);
    exit(2);
  }

  /* Disable secure memory.  */
  gcry_control(GCRYCTL_DISABLE_SECMEM, 0);

  /* ... If required, other initialization goes here.  */

  /* Tell Libgcrypt that initialization has completed. */
  gcry_control(GCRYCTL_INITIALIZATION_FINISHED, 0);
}
}
