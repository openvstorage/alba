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

#include "statistics.h"
#include "stuff.h"

namespace alba {
namespace statistics {

Statistics::Statistics()
    : _n_samples(0), _min_dur{1000000000.0}, _max_dur{0.0}, _avg(0.0),
      _borders{100, 125, 150, 175, 200, 225,
               250, 300, 350, 400, 800, 1000000000.0},
      _dur_buckets(_borders.size()) {
  _last_index = _borders.size() - 1;
}

void Statistics::new_start() { _t0 = high_resolution_clock::now(); }
void Statistics::new_stop() {

  _t1 = high_resolution_clock::now();

  int duration = duration_cast<microseconds>(_t1 - _t0).count();

  if (duration < _min_dur) {
    _min_dur = duration;
  }
  if (duration > _max_dur) {
    _max_dur = duration;
  }
  uint border_index = 0;
  bool set = false;

  while (border_index < _last_index) {
    double border = _borders[border_index];
    if (duration < border) {
      _dur_buckets[border_index] = _dur_buckets[border_index] + 1;
      set = true;
      break;
    }
    border_index++;
  }
  if (!set) {
    _dur_buckets[_last_index] = _dur_buckets[_last_index] + 1;
  }
  _avg = ((_avg * _n_samples) + duration) / (_n_samples + 1);
  _n_samples++;
}

void Statistics::pretty(std::ostream &os) const {
  os << "n_samples: " << _n_samples << std::endl;
  os << "min_dur: " << _min_dur << std::endl;
  os << "max_dur: " << _max_dur << std::endl;
  os << "average: " << _avg << std::endl;

  double duration =
      (duration_cast<milliseconds>(_t1 - _creation).count()) / 1000.0;
  os << "total_time: " << duration << " s" << std::endl;

  double frequency = ((double)_n_samples) / (duration + .0000001);
  os << "frequency: " << frequency << "/s" << std::endl;

  uint border_index = 0;
  while (border_index <= _last_index) {
    os << _dur_buckets[border_index] << "\t<" << _borders[border_index]
       << std::endl;
    border_index++;
  };
}

std::ostream &operator<<(std::ostream &os, const Statistics &s) {
  using alba::stuff::operator<<;
  os << "Statistics{";
  os << " _n_samples = " << s._n_samples;
  os << ", _min_dur = " << s._min_dur;
  os << ", _max_dur = " << s._max_dur;
  os << ", _avg = " << s._avg;
  os << ", _dur_buckets" << s._dur_buckets;
  os << ", _borders" << s._borders;
  os << " }";
  return os;
}
}
}
