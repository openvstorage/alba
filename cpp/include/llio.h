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
#include "alba_logger.h"
#include <istream>
#include <ostream>
#include <sstream>
#include <vector>
#include <string>
#include <boost/optional.hpp>

namespace alba {
namespace llio {

struct stream_exception : virtual std::exception {
  stream_exception(const std::string &what) : _what(what) {}

  virtual const char *what() const noexcept { return _what.c_str(); }

  std::string _what;
};

struct input_stream_exception : virtual stream_exception {
  input_stream_exception(const std::string &what) : stream_exception(what) {}
};

struct output_stream_exception : virtual stream_exception {
  output_stream_exception(const std::string &what) : stream_exception(what) {}
};

struct deserialisation_exception : virtual std::exception {
  deserialisation_exception(const std::string &what) : _what(what) {}

  virtual const char *what() const noexcept { return _what.c_str(); }

  std::string _what;
};

void check_stream(const std::istream &is);

class message {
public:
  message(std::function<void(char *, const int)> reader) {
    uint32_t size;
    reader((char *)&size, sizeof(uint32_t));
    _buffer.resize(size);
    reader(_buffer.data(), size);
    _pos = 0;
  }

  message(std::istream &is) {
    uint32_t size;
    is.read((char *)&size, 4);
    check_stream(is);
    _buffer.resize(size);
    is.read(_buffer.data(), size);
    check_stream(is);
    _pos = 0;
  }

  message(std::vector<char> &buffer) {
    _buffer = buffer;
    _pos = 0;
  }

  const char *current(size_t len) {
    uint32_t size = _buffer.size();
    if (_pos + len > size) {
      ALBA_LOG(WARNING, "WARNING: _pos:" << _pos << " + " << len << " > " << size);
      throw deserialisation_exception("reading outside of message");
    }
    return &_buffer.data()[_pos];
  }

  void skip(int x) { _pos += x; }

private:
  std::vector<char> _buffer;
  uint32_t _pos;
};

class message_builder {
public:
  message_builder() : _buffer() {
    const uint32_t size_res = 0x77777777;
    const char *size_resp = (const char *)(&size_res);
    _buffer.write(size_resp, 4);
  }

  void output_using(
      /*void (*writer) (const char* buffer,  const int len)*/
      std::function<void(const char *, const int)> writer) {
    const std::string bs = _buffer.str();
    uint32_t bs_size = bs.size();
    uint32_t size = bs_size - 4;
    const char *data = bs.data();
    // DIRTY: "trust me, I know what I'm doing (TM)"
    uint32_t *data_mut = (uint32_t *)data;
    data_mut[0] = size;
    writer(data, bs_size);
  }

  void output(std::ostream &os) {
    output_using([&](const char *buffer, const int len)
                     -> void { os.write(buffer, len); });
    os.flush();
    if (!os) {
      throw output_stream_exception("invalid outputstream");
    }
  }

  void add_raw(const char *b, uint32_t size) noexcept {
    _buffer.write(b, size);
  }

  void add_type(const uint8_t i) noexcept {
    const char *ip = (const char *)(&i);
    add_raw(ip, 1);
  }

  std::string as_string() noexcept { return _buffer.str(); }

private:
  std::ostringstream _buffer;
};

template <typename T> void to(message_builder &mb, const T &) noexcept;

template <typename T> void from(message &m, T &t);

template <typename T>
void to(message_builder &mb, const std::vector<T> &ts) noexcept {
  uint32_t size = ts.size();
  to(mb, size);
  for (auto iter = ts.rbegin(); iter != ts.rend(); ++iter) {
    to(mb, *iter);
  }
}

void to_be(message_builder &mb, const uint32_t &i) noexcept;

template <typename T> void from(message &m, std::vector<T> &ts) noexcept {

  uint32_t size;
  from(m, size);
  ts.resize(size);

  for (int32_t i = size - 1; i >= 0; --i) {
    T t;
    from(m, t);
    ts[i] = t;
  }
}

template <typename X, typename Y>
void to(message_builder &mb, const std::pair<X, Y> &p) noexcept {
  to(mb, p.first);
  to(mb, p.second);
}

template <typename X, typename Y>
void from(message &m, std::pair<X, Y> &p) noexcept {
  from(m, p.first);
  from(m, p.second);
}

template <typename X, typename Y, typename Z>
void from(message &m, std::tuple<X, Y, Z> &t) noexcept {
  from(m, std::get<0>(t));
  from(m, std::get<1>(t));
  from(m, std::get<2>(t));
}

template <typename X>
void to(message_builder &mb, const boost::optional<X> &xo) noexcept {
  if (boost::none == xo) {
    to(mb, false);
  } else {
    to(mb, true);
    to(mb, *xo);
  }
}

template <typename X> void from(message &m, boost::optional<X> &xo) noexcept {
  bool d;
  from(m, d);
  if (d) {
    X x;
    from(m, x);
    xo = x;
  } else {
    xo = boost::none;
  }
}
}
}
