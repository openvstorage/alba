(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

module Stat = struct
    type stat = {
        n   : int64;
        avg : float;
        exp_avg: float;
        m2  : float;
        var : float;
        min : float;
        max : float;
        alpha : float; (* factor for exp smoothing *)
      } [@@deriving show, yojson]

    let make ?(alpha=0.9) () = {
        n = 0L;
        avg = 0.0;
        exp_avg = 0.0;
        m2 = 0.0;
        var = 0.0;
        min = max_float;
        max = min_float;
        alpha;
      }

    let _update old delta =
      let n' = Int64.succ old.n  in
      let old_nf = Int64.to_float old.n in
      let diff = delta -. old.avg in
      let avg' = old.avg +. (diff /. Int64.to_float n') in
      let alpha = old.alpha in
      (* http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm *)
      let m2' = old.m2 +. diff *. (delta -. avg') in
      let var' = if n' < 2L then 0.0 else m2' /. old_nf in
      let exp_avg' = (old.exp_avg +. delta) *. alpha in
      let min' = min old.min delta in
      let max' = max old.max delta in
      { n = n';
        avg = avg';
        exp_avg = exp_avg';
        m2 = m2';
        var = var';
        min = min'; max = max';
        alpha;
      }

    let stat_to buf stat =
      Llio.int64_to buf stat.n;
      Llio.float_to buf stat.avg;
      Llio.float_to buf stat.exp_avg;
      Llio.float_to buf stat.m2;
      Llio.float_to buf stat.var;
      Llio.float_to buf stat.min;
      Llio.float_to buf stat.max;
      Llio.float_to buf stat.alpha

    let stat_from buf =
      let n       = Llio.int64_from buf in
      let avg     = Llio.float_from buf in
      let exp_avg = Llio.float_from buf in
      let m2      = Llio.float_from buf in
      let var     = Llio.float_from buf in
      let min     = Llio.float_from buf in
      let max     = Llio.float_from buf in
      let alpha   = Llio.float_from buf in
      { n;avg;exp_avg;m2;var;min;max; alpha}
  end
