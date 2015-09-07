/*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
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
  case AlbaLogLevel::WARNING:
    os << "WARNING";
    break;
  }
  return os;
}

std::function<std::function<void(AlbaLogLevel, std::string &)> *(AlbaLogLevel)>
logFunc_;

void setLogFunction(std::function<std::function<
    void(AlbaLogLevel, std::string &)> *(AlbaLogLevel)> logFunc) {
  logFunc_ = logFunc;
}

std::function<void(AlbaLogLevel, std::string &)> *
getLogger(AlbaLogLevel level) {
  return logFunc_(level);
}
}
}
