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

#include "llio.h"

namespace alba {
namespace llio {

void check_stream(const std::istream &is) {
  if (!is) {
    throw stream_exception("invalid inputstream");
  }
}

template <> void to(message_builder &mb, const bool &b) noexcept {
  char c = b ? '\01' : '\00';
  mb.add_raw(&c, 1);
}

template <> void from(message &m, uint8_t &i) {
  i = *(m.current());
  m.skip(1);
}

template <> void from(message &m, bool &b) {

  char c;
  const char *ib = m.current();
  c = *ib;
  switch (c) {
  case '\01': {
    b = true;
  }; break;
  case '\00': {
    b = false;
  }; break;
  default:
    throw deserialisation_exception(
        "got unexpected value while deserializing a boolean");
  }
  m.skip(1);
}

template <> void to(message_builder &mb, const uint32_t &i) noexcept {
  const char *ip = (const char *)(&i);
  mb.add_raw(ip, 4);
}

template <> void from<uint32_t>(message &m, uint32_t &i) {

  const char *ib = m.current();
  i = *((uint32_t *)ib);
  m.skip(4);
}

template <> void from<int32_t>(message&m, int32_t &i){
  const char *ib = m.current();
  i = *((int32_t *)ib);
  m.skip(4);
}

template <> void to(message_builder &mb, const uint64_t &i) noexcept {
  const char *ip = (const char *)(&i);
  mb.add_raw(ip, 8);
}

template <> void from(message &m, uint64_t &i) {
  const char *ib = m.current();
  i = *((uint64_t *)ib);
  m.skip(8);
}

template <> void to(message_builder &mb, const std::string &s) noexcept {
  uint32_t size = s.size();
  to(mb, size);
  mb.add_raw(s.data(), size);
}

template <> void from(message &m, std::string &s) {
  uint32_t size;
  from<uint32_t>(m, size);
  const char *ib = m.current();
  s.replace(0, size, ib, size);
  s.resize(size);
  m.skip(size);
}
}
}
