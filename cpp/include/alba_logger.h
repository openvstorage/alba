/*
Copyright 2015 Open vStorage NV

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

#ifndef ALBA_LOGGER
#define ALBA_LOGGER

#include <iostream>
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
#endif
