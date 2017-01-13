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
#include <boost/optional.hpp>
#include <istream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <string.h>
#include <vector>
#include "stuff.h"

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
  template<typename R>
  static message from_reader(R&& reader) {
    uint32_t size;
    reader((char *)&size, sizeof(uint32_t));
    message m(size);
    reader(m._buffer.data(), size);
    return m;
  }

  message(std::istream &is) {
    uint32_t size;
    is.read((char *)&size, 4);
    check_stream(is);
    _buffer.resize(size);
    is.read(_buffer.data(), size);
    check_stream(is);
    _pos = 0;
    _initial_offset = 0;
    _size = size;
  }

  message(std::vector<char> &buffer) : message(buffer, 0, buffer.size()) {}

  message(std::vector<char> &buffer, size_t offset, size_t size) {
    _buffer = buffer;// this copies.
    _initial_offset = offset;
    _pos = offset;
    _size = size;
  }

  const char *current(size_t len) {
    if (_pos + len > _initial_offset + _size) {
      ALBA_LOG(WARNING, "WARNING: _pos:" << _pos << " + " << len << " > "
                                         << _initial_offset << " + " << _size);
      throw deserialisation_exception(
          "message.current(): reading outside of message");
    }
    return &_buffer.data()[_pos];
  }

  message get_nested_message(uint32_t len) {
    if (_pos + len > _initial_offset + _size) {
      ALBA_LOG(WARNING, "WARNING: _pos:" << _pos << " + " << len << " > "
                                         << _initial_offset << " + " << _size);
      throw deserialisation_exception(
          "message.get_nested_message(): reading outside of message");
    }
    return message(_buffer, _pos, len);
  }

  uint32_t get_pos() { return _pos; }

  void skip(int x) { _pos += x; }

  size_t size() const noexcept { return _size; }

  void dump(std::ostream &os) {
    os.write((char *)&_size, sizeof(uint32_t));
    os.write((char *)&_buffer.data()[_initial_offset], _size);
  }

private:
  std::vector<char> _buffer;
  size_t _pos;
  size_t _initial_offset;
  size_t _size;

  message(const size_t size){
      _pos = 0;
      _size = size;
      _initial_offset = 0;
      _buffer.resize(size);
  }
};

class message_builder {
public:
message_builder() : _size(_SIZE0), _buffer{new char[_SIZE0]}, _pos(4) {
        // keep valgrind happy:
        uint32_t* p = (uint32_t*)_buffer;
        p[0] = 0;
  }

  template<typename W>
  void output_using(W&& writer) {
    uint32_t size = _pos - 4;
    uint32_t* p = (uint32_t*) _buffer;
    p[0] = size;
    writer(_buffer, _pos);
  }

  void output(std::ostream &os) {
    output_using([&](const char *buffer, const int len) -> void {
      os.write(buffer, len);
    });
    os.flush();
    if (!os.good()) {
      throw output_stream_exception("invalid outputstream");
    }
  }

  void add_raw(const char *b, uint32_t len) noexcept {
      uint free = _size - _pos;
      if(free < len){
          uint new_size = _size + std::max (len, _size);
          ALBA_LOG(DEBUG, free << " < " << len
                   << " => grow from " << _size
                   << " to " << new_size);
          char* new_buffer = new char[new_size];
          memcpy(new_buffer,_buffer, _pos);
          _size = new_size;
          delete [] _buffer;
          _buffer = new_buffer;
      }
      memcpy(&_buffer[_pos], b, len);
      _pos += len;
  }

  void add_type(const uint8_t i) noexcept {
    const char *ip = (const char *)(&i);
    add_raw(ip, 1);
  }

  std::string as_string() noexcept {
      return std::string(_buffer, _pos);
  }

  void reset() noexcept {
      _pos = 4;
  }

  ~message_builder(){
      delete [] _buffer;
  }

private:
  uint32_t _size;
  char* _buffer;
  uint32_t _pos = 0;
  static const uint32_t _SIZE0 = 32;
};

template <typename T> void to(message_builder &mb, const T &) noexcept;

template <typename T> void from(message &m, T &t);

/*
  ok_to_continue:
    in case of a deserialization exception,
    signal that the message can still be used to read the next value

 */
template <typename T> void from2(message &m, T &t, bool &ok_to_continue);

template <typename T>
void to(message_builder &mb, const std::vector<T> &ts) noexcept {
  uint32_t size = ts.size();
  to(mb, size);
  for (auto iter = ts.rbegin(); iter != ts.rend(); ++iter) {
    to(mb, *iter);
  }
}

void to_be(message_builder &mb, const uint32_t &i) noexcept;
void to_be(message_builder &mb, const uint64_t &i) noexcept;

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

template <typename X>
void to(message_builder &mb, const std::shared_ptr<X> &x) noexcept {
  to(mb, *x);
}

template <typename X>
void to(message_builder &mb, const std::unique_ptr<X> &x) noexcept {
  to(mb, *x);
}
}
}
