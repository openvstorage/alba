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
#include <vector>
#include <chrono>
#include <iostream>

namespace alba {
namespace statistics {

using namespace std::chrono;

struct Statistics {
  Statistics();

  void new_start();
  void new_stop();

  void pretty(std::ostream &os) const;

private:
  uint _n_samples;
  double _min_dur;
  double _max_dur;
  double _avg;
  std::vector<double> _borders;
  uint _last_index;
  high_resolution_clock::time_point _t0;
  high_resolution_clock::time_point _t1;
  high_resolution_clock::time_point _creation = high_resolution_clock::now();
  ;
  std::vector<double> _dur_buckets;
  friend std::ostream &operator<<(std::ostream &os, const Statistics &);
};
std::ostream &operator<<(std::ostream &os, const Statistics &);
}
}
