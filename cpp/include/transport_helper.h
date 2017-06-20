/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include "transport.h"

namespace alba {
namespace transport {

std::unique_ptr<Transport>
make_transport(const Kind, const std::string &ip, const std::string &port,
               const std::chrono::steady_clock::duration &timeout);
}
}
