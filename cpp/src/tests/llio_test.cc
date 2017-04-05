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
#include "stuff.h"
#include "gtest/gtest.h"
#include <boost/log/trivial.hpp>
#include <boost/optional.hpp>
#include <sstream>
#include <string>
#include <vector>

using namespace alba::llio;

TEST(llio, composition) {
  message_builder mb;
  std::string my_string("0123456789a");
  std::pair<std::string, bool> psb(my_string, true);
  auto psbo = boost::optional<std::pair<std::string, bool>>(psb);
  std::vector<boost::optional<std::pair<std::string, bool>>> xs;
  xs.push_back(psbo);
  to(mb, xs);

  std::ostringstream sos;
  mb.output(sos);
  std::string contents = sos.str();
  BOOST_LOG_TRIVIAL(debug) << "contents : " << contents.size() << " bytes";

  alba::stuff::dump_buffer(std::cout, contents.data(), contents.size());
  std::cout << std::endl;

  std::istringstream sis(contents);
  std::vector<boost::optional<std::pair<std::string, bool>>> ys;
  auto buffer = message_buffer::from_istream(sis);

  message m(buffer);
  from(m, ys);
  EXPECT_EQ(xs.size(), ys.size());
  auto y0 = ys[0];
  auto py0 = *y0;
  std::string y0s = py0.first;
  bool y0b = py0.second;
  std::cout << "y0s=" << y0s << " size:" << y0s.size() << std::endl;
  alba::stuff::dump_buffer(std::cout, y0s.data(), y0s.size());
  std::cout << std::endl;
  EXPECT_EQ(y0s, my_string);
  EXPECT_EQ(y0b, true);
}

static std::vector<uint64_t> integers{
    0,
    1,
    2,
    127,
    128,
    129,
    16383,
    16384,
    16385,
    40000,
    100000,
    4904328,
    3890292099,
    3820908392820902,
    9223372036854775807,
    std::numeric_limits<uint64_t>::max(),
};

TEST(llio, varint) {

  integers.push_back((uint64_t)std::rand());
  integers.push_back((uint64_t)std::rand());
  integers.push_back((uint64_t)std::rand());

  for (uint64_t t : integers) {
    message_builder mb;
    varint_t v;
    v.j = t;
    to(mb, v);
    std::ostringstream sos;
    mb.output(sos);
    std::string contents = sos.str();
    alba::stuff::dump_buffer(std::cout, contents.data(), contents.size());
    std::cout << std::endl;

    std::istringstream sis(contents);
    auto buffer = message_buffer::from_istream(sis);
    message m(buffer);
    varint_t v2;
    from(m, v2);
    EXPECT_EQ(v.j, v2.j);
  }
}

TEST(llio, to_from_be) {
  for (uint64_t t : integers) {
    message_builder mb;
    to_be(mb, t);
    std::ostringstream sos;
    mb.output(sos);
    std::string contents = sos.str();
    alba::stuff::dump_buffer(std::cout, contents.data(), contents.size());
    std::cout << std::endl;

    std::istringstream sis(contents);
    auto buffer = message_buffer::from_istream(sis);
    message m(buffer);
    uint64_t res;
    from_be(m, res);
    EXPECT_EQ(t, res);
  }
}
