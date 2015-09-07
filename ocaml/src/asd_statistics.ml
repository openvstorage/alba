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

open Stat

module AsdStatistics = struct
    let section = Alba_statistics.Statistics.section
    include Stat

    type t = {
        mutable multi_get     : stat;
        mutable range         : stat;
        mutable range_entries : stat;
        mutable apply         : stat;
        mutable creation      : float;
        mutable period        : float;
      } [@@deriving show, yojson]

    let make () =
      let creation = Unix.gettimeofday() in
      {
        creation;
        period          = 0.0;
        multi_get     = Stat.make ();
        range         = Stat.make ();
        range_entries = Stat.make ();
        apply         = Stat.make ();


      }
    let clone t = { t with multi_get = t.multi_get }

    let with_timing = Alba_statistics.Statistics.with_timing
    let with_timing_lwt = Alba_statistics.Statistics.with_timing_lwt

    let new_range t delta = t.range  <- _update t.range delta
    let new_multi_get t delta = t.multi_get <- _update t.multi_get delta


    let new_apply t delta = t.apply <- _update t.apply delta


    let new_range_entries t delta =
      t.range_entries <- _update t.range_entries delta

    let to_buffer buf t =
      let ser_version = 1 in Llio.int8_to buf ser_version;
      Llio.float_to buf t.creation;
      Llio.float_to buf t.period;
      stat_to buf t.multi_get;
      stat_to buf t.range;
      stat_to buf t.range_entries;
      stat_to buf t.apply


    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let creation  = Llio.float_from buf in
      let period    = Llio.float_from buf in
      let multi_get = stat_from buf in
      let range = stat_from buf in
      let range_entries = stat_from buf in
      let apply = stat_from buf in
      { creation; period; multi_get; range ; range_entries; apply}

    let clear t =
      t.creation      <- Unix.gettimeofday ();
      t.period        <- 0.0;
      t.multi_get     <- Stat.make ();
      t.range         <- Stat.make ();
      t.range_entries <- Stat.make ();
      t.apply         <- Stat.make ()

    let stop t = t.period <- Unix.gettimeofday() -. t.creation
end
