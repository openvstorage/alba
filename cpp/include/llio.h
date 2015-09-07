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

#ifndef ALBA_LLIO_H
#define ALBA_LLIO_H

#include <istream>
#include <ostream>
#include <sstream>
#include <vector>
#include <string>
#include <boost/optional.hpp>

namespace alba {
namespace llio {

struct stream_exception : virtual std::exception {
  stream_exception(const std::string& what) : _what(what) {}

  virtual const char *what() const noexcept { return _what.c_str(); }

  std::string _what;
};

struct input_stream_exception : virtual stream_exception {
  input_stream_exception(const std::string& what) : stream_exception(what) {}
};

struct output_stream_exception : virtual stream_exception {
  output_stream_exception(const std::string& what) : stream_exception(what) {}
};

struct deserialisation_exception : virtual std::exception {
  deserialisation_exception(const std::string& what) : _what(what) {}

  virtual const char *what() const noexcept { return _what.c_str(); }

  std::string _what;
};

void check_stream(const std::istream &is);

class message {
public:
  message(std::istream &is) {
    uint32_t size;
    is.read((char *)&size, 4);
    check_stream(is);
    _buffer.resize(size);
    is.read(_buffer.data(), size);
    check_stream(is);
    _pos = 0;
  }
  const char *current() { return &_buffer.data()[_pos]; }

  void skip(int x) { _pos += x; }

  // private:
  std::vector<char> _buffer;
  uint32_t _pos;
};

class message_builder {
public:
  message_builder() : _buffer() {
     const uint32_t size_res = 0x77777777;
     const char* size_resp = (const char*)(&size_res);
     _buffer.write(size_resp,4);
  }

  void output(std::ostream &os) {
    const std::string bs = _buffer.str();
    uint32_t size = bs.size() - 4;
    const char* data = bs.data();
    //DIRTY: "trust me, I know what I'm doing (TM)"
    uint32_t* data_mut = (uint32_t*) data;
    data_mut[0] = size;

    os.write(data, size + 4);
    os.flush();
    if (!os) {
      throw output_stream_exception("invalid outputstream");
    }
  }
  void add_raw(const char *b, uint32_t size) noexcept {
    _buffer.write(b, size);
  }

  void add_type(const uint8_t i) noexcept{
    const char *ip = (const char *)(&i);
    add_raw(ip, 1);
  }

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
#endif
