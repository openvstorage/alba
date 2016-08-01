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

#include "llio.h"

namespace alba {
namespace llio {

void check_stream(const std::istream &is) {
  if (!is) {
    throw input_stream_exception("invalid inputstream");
  }
}

template <> void to(message_builder &mb, const bool &b) noexcept {
  char c = b ? '\01' : '\00';
  mb.add_raw(&c, 1);
}

template <> void from(message &m, uint8_t &i) {
  const char *ib = m.current(1);
  i = *((uint8_t *)ib);
  m.skip(1);
}

template <> void from(message &m, bool &b) {

  char c;
  const char *ib = m.current(1);
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

void to_be(message_builder &mb, const uint32_t &i) noexcept {
  uint32_t res =
      (i >> 24) | ((i << 8) & 0x00ff0000) | ((i >> 8) & 0x0000ff00) | (i << 24);
  to(mb, res);
}

template <> void from<uint32_t>(message &m, uint32_t &i) {

  const char *ib = m.current(4);
  i = *((uint32_t *)ib);
  m.skip(4);
}

template <> void from<int32_t>(message &m, int32_t &i) {
  const char *ib = m.current(4);
  i = *((int32_t *)ib);
  m.skip(4);
}

template <> void to(message_builder &mb, const uint64_t &i) noexcept {
  const char *ip = (const char *)(&i);
  mb.add_raw(ip, 8);
}

template <> void from(message &m, uint64_t &i) {
  const char *ib = m.current(8);
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
  const char *ib = m.current(size);
  s.replace(0, size, ib, size);
  s.resize(size);
  m.skip(size);
}

template <> void to(message_builder &mb, const double &d) noexcept {
  const char *dp = (const char *)(&d);
  mb.add_raw(dp, 8);
}

template <> void from(message &m, double &d) {
  const char *db = m.current(8);
  d = *((double *)db);
  m.skip(8);
}
}
}
