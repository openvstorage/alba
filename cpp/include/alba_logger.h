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

#include <iostream>
#include <sstream>
#include <functional>

namespace alba {
namespace logger {

enum class AlbaLogLevel { DEBUG, INFO, WARNING };

std::ostream &operator<<(std::ostream &os, const AlbaLogLevel);

void setLogFunction(std::function<
    std::function<void(AlbaLogLevel, std::string &)> *(AlbaLogLevel)> logFunc);
std::function<void(AlbaLogLevel, std::string &)> *getLogger(AlbaLogLevel);

#define ALBA_LOG(level, message)                                               \
  {                                                                            \
    std::function<void(alba::logger::AlbaLogLevel, std::string &)> *logger =   \
        alba::logger::getLogger(alba::logger::AlbaLogLevel::level);            \
    if (nullptr != logger) {                                                   \
      std::ostringstream os;                                                   \
      os << message;                                                           \
      std::string msg = os.str();                                              \
      (*logger)(alba::logger::AlbaLogLevel::level, msg);                       \
    }                                                                          \
  }
}
}
