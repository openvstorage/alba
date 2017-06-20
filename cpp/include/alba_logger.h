/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include <functional>
#include <iostream>
#include <sstream>

namespace alba {
namespace logger {

enum class AlbaLogLevel { DEBUG, INFO, ERROR, WARNING };

std::ostream &operator<<(std::ostream &os, const AlbaLogLevel);

void setLogFunction(
    std::function<
        std::function<void(AlbaLogLevel, std::string &)> *(AlbaLogLevel)>
        logFunc);
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
