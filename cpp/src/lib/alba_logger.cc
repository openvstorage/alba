/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#include "alba_logger.h"

namespace alba {
namespace logger {

std::ostream &operator<<(std::ostream &os, const AlbaLogLevel level) {
  switch (level) {
  case AlbaLogLevel::DEBUG:
    os << "DEBUG";
    break;
  case AlbaLogLevel::INFO:
    os << "INFO";
    break;
  case AlbaLogLevel::ERROR:
    os << "ERROR";
    break;
  case AlbaLogLevel::WARNING:
    os << "WARNING";
    break;
  }
  return os;
}

std::function<std::function<void(AlbaLogLevel, std::string &)> *(AlbaLogLevel)>
    logFunc_;

void setLogFunction(
    std::function<
        std::function<void(AlbaLogLevel, std::string &)> *(AlbaLogLevel)>
        logFunc) {
  logFunc_ = logFunc;
}

std::function<void(AlbaLogLevel, std::string &)> *
getLogger(AlbaLogLevel level) {
  return logFunc_(level);
}
}
}
