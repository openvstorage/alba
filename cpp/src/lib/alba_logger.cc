/*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
