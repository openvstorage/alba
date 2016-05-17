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

#include "gtest/gtest.h"
#include "llio.h"
#include "stuff.h"
#include <string>
#include <vector>
#include <boost/optional.hpp>
#include <boost/log/trivial.hpp>
#include <sstream>

TEST(llio, composition) {
  using namespace alba::llio;
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
  message m(sis);
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
