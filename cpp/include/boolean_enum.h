/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include <boost/logic/tribool.hpp>
#include <iostream>

#define DECLARE_BOOLEAN_ENUM(name) enum class name : bool;

#define BOOLEAN_ENUM(name)                                                     \
  enum class name : bool { F = false, T = true };                              \
                                                                               \
  inline std::ostream &operator<<(std::ostream &os, const name &in) {          \
    return os << (in == name::T ? (#name "::T") : (#name "::F"));              \
  }                                                                            \
                                                                               \
  inline bool T(const name &in) { return in == name::T; }                      \
  inline bool BooleanEnumTrue(const name &in) { return in == name::T; }        \
                                                                               \
  inline bool F(const name &in) { return in == name::F; }                      \
  inline bool BooleanEnumFalse(const name &in) { return in == name::F; }       \
                                                                               \
  inline std::istream &operator>>(std::istream &is, name &out) {               \
    static const std::string e_name(#name "::");                               \
    char c;                                                                    \
    for (size_t i = 0; i < e_name.size(); ++i) {                               \
      is >> c;                                                                 \
      if (not is or c != e_name[i]) {                                          \
        is.setstate(std::ios_base::badbit);                                    \
        return is;                                                             \
      }                                                                        \
    }                                                                          \
    is >> c;                                                                   \
    if (not is) {                                                              \
      is.setstate(std::ios_base::badbit);                                      \
      return is;                                                               \
    }                                                                          \
    switch (c) {                                                               \
    case 'T':                                                                  \
      out = name::T;                                                           \
      return is;                                                               \
    case 'F':                                                                  \
      out = name::F;                                                           \
      return is;                                                               \
    default:                                                                   \
      is.setstate(std::ios_base::badbit);                                      \
      return is;                                                               \
    }                                                                          \
  }
