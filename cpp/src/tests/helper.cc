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
#include "helper.h"

void alba::_compare(proxy_protocol::lookup_result_t &exp,
                    proxy_protocol::lookup_result_t &act) {

  EXPECT_EQ(exp.chunk_index, act.chunk_index);
  EXPECT_EQ(exp.fragment_index, act.fragment_index);
  EXPECT_EQ(exp.pos_in_fragment, act.pos_in_fragment);
  EXPECT_EQ(exp.fragment_length, act.fragment_length);
  EXPECT_EQ(exp.fragment_version, act.fragment_version);
  EXPECT_EQ(exp._osd, act._osd);
}
