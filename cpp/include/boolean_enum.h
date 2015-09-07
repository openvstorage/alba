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

#ifndef BOOLEAN_ENUM_H_
#define BOOLEAN_ENUM_H_
#include <iostream>
#include <boost/logic/tribool.hpp>

#define DECLARE_BOOLEAN_ENUM(name) enum class name : bool;


#define BOOLEAN_ENUM(name)                                              \
    enum class name : bool                                              \
    {                                                                   \
        F = false,                                                      \
            T = true                                                    \
            };                                                          \
                                                                        \
    inline std::ostream&                                                \
    operator<<(std::ostream& os,                                        \
               const name& in)                                          \
    {                                                                   \
        return os << (in == name::T ? (#name "::T") : (#name "::F"));   \
    }                                                                   \
                                                                        \
    inline bool                                                         \
    T(const name& in)                                                   \
    {                                                                   \
        return in == name::T;                                           \
    }                                                                   \
    inline bool                                                         \
    BooleanEnumTrue(const name& in)                                     \
    {                                                                   \
        return in == name::T;                                           \
    }                                                                   \
                                                                        \
    inline bool                                                         \
    F(const name& in)                                                   \
    {                                                                   \
        return in == name::F;                                           \
    }                                                                   \
    inline bool                                                         \
    BooleanEnumFalse(const name& in)                                    \
    {                                                                   \
        return in == name::F;                                           \
    }                                                                   \
                                                                        \
    inline std::istream&                                                \
    operator>>(std::istream& is,                                        \
               name& out)                                               \
    {                                                                   \
    static const std::string e_name(#name "::");                        \
    char c;                                                             \
    for(size_t i = 0; i< e_name.size(); ++i)                            \
    {                                                                   \
        is >> c;                                                        \
        if(not is or                                                    \
           c != e_name[i])                                              \
        {                                                               \
            is.setstate(std::ios_base::badbit);                         \
            return is;                                                  \
        }                                                               \
    }                                                                   \
    is >> c;                                                            \
    if(not is)                                                          \
    {                                                                   \
        is.setstate(std::ios_base::badbit);                             \
        return is;                                                      \
    }                                                                   \
    switch(c)                                                           \
    {                                                                   \
    case 'T':                                                           \
        out = name ::T;                                                 \
        return is;                                                      \
    case 'F':                                                           \
        out = name ::F;                                                 \
        return is;                                                      \
    default:                                                            \
        is.setstate(std::ios_base::badbit);                             \
        return is;                                                      \
    }                                                                   \
    }




#endif // BOOLEAN_H_
