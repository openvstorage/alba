/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once
#include <chrono>
#include <iostream>
#include <vector>

namespace alba {
namespace statistics {

using namespace std::chrono;

struct RoraCounter {
  uint64_t fast_path;
  uint64_t slow_path;

  RoraCounter() : fast_path(0L), slow_path(0L) {}
};

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
