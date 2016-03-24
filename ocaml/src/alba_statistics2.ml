(*
Copyright 2016 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

open Prelude
open Stat

type stat = Stat.stat [@@deriving show, yojson]

type ns_t = {
    mutable upload: stat;
    mutable download: stat;
    mutable manifest_cached: int;
    mutable manifest_from_nsm  : int;
    mutable manifest_stale : int;
    mutable fragment_cache_hits: int;
    mutable fragment_cache_misses:int;
    mutable partial_read_size: stat;
    mutable partial_read_count: stat;
    mutable partial_read_time : stat;
    mutable partial_read_objects: stat;
  }[@@ deriving show, yojson]

let ns_make () =
  { upload = Stat.make();
    download = Stat.make();
    manifest_cached = 0;
    manifest_from_nsm  = 0;
    manifest_stale = 0;
    fragment_cache_hits = 0;
    fragment_cache_misses = 0;
    partial_read_size    = Stat.make ();
    partial_read_count   = Stat.make ();
    partial_read_time    = Stat.make ();
    partial_read_objects = Stat.make ();
  }

let ns_to buf t =
  let module Llio = Llio2.WriteBuffer in
  Stat_deser.to_buffer' buf t.upload;
  Stat_deser.to_buffer' buf t.download;
  Llio.int_to buf t.manifest_cached;
  Llio.int_to buf t.manifest_from_nsm;
  Llio.int_to buf t.manifest_stale;
  Llio.int_to buf t.fragment_cache_hits;
  Llio.int_to buf t.fragment_cache_misses;

  Stat_deser.to_buffer' buf t.partial_read_size;
  Stat_deser.to_buffer' buf t.partial_read_count;
  Stat_deser.to_buffer' buf t.partial_read_time;
  Stat_deser.to_buffer' buf t.partial_read_objects

let ns_from buf =
  let module Llio = Llio2.ReadBuffer in
  let upload   = Stat_deser.from_buffer' buf in
  let download = Stat_deser.from_buffer' buf in
  let manifest_cached    = Llio.int_from buf in
  let manifest_from_nsm  = Llio.int_from buf in
  let manifest_stale     = Llio.int_from buf in
  let fragment_cache_hits    = Llio.int_from buf in
  let fragment_cache_misses  = Llio.int_from buf in
  (* trick to be able to work with <= 0.6.20 proxies *)
  let (partial_read_size,
       partial_read_count,
       partial_read_time,
       partial_read_objects)
    =
    begin
      if Llio.buffer_done buf
      then
        let r = Stat.make () in
        r,r,r,r
      else
        let s = Stat_deser.from_buffer' buf in
        let c = Stat_deser.from_buffer' buf in
        let t = Stat_deser.from_buffer' buf in
        if Llio.buffer_done buf
        then s,c,t,Stat.make()
        else
          let n = Stat_deser.from_buffer' buf in
          s,c,t,n
    end
  in

  { upload ; download;
    manifest_cached;
    manifest_from_nsm;
    manifest_stale;
    fragment_cache_hits;
    fragment_cache_misses;
    partial_read_size;
    partial_read_count;
    partial_read_time;
    partial_read_objects;
  }

type t = {
    mutable creation:timestamp;
    mutable period: float;
    ns_stats : (string, ns_t) Prelude.Hashtbl.t;
  } [@@deriving show, yojson]

type t' = {
    t : t;
    changed_ns_stats : (string, unit) Hashtbl.t;
  }

let make () =
  let creation = Unix.gettimeofday () in
  { t = { creation; period = 0.0;
          ns_stats = Hashtbl.create 3;
        };
    changed_ns_stats = Hashtbl.create 3;
  }

let to_buffer buf t =
  let module Llio = Llio2.WriteBuffer in
  let ser_version = 1 in Llio.int8_to buf ser_version;
                         Llio.float_to buf t.creation;
                         Llio.float_to buf t.period;
                         Llio.hashtbl_to Llio.string_to ns_to buf t.ns_stats

let from_buffer buf =
  let module Llio = Llio2.ReadBuffer in
  let ser_version = Llio.int8_from buf in
  assert (ser_version = 1);
  let creation = Llio.float_from buf in
  let period   = Llio.float_from buf in
  let ns_stats = Llio.hashtbl_from Llio.string_from ns_from buf in
  {creation;period;ns_stats}


let deser = from_buffer, to_buffer

let stop t = t.period <- Unix.gettimeofday() -. t.creation

let clone t = { t.t with creation = t.t.creation }

let clear t =
  Hashtbl.clear t.changed_ns_stats;
  t.t.creation <- Unix.gettimeofday ();
  t.t.period <- 0.0;
  Hashtbl.clear t.t.ns_stats

let find t ns =
  Hashtbl.replace t.changed_ns_stats ns ();
  try Hashtbl.find t.t.ns_stats ns
  with Not_found ->
    let v = ns_make () in
    let () = Hashtbl.add t.t.ns_stats ns v in
    v

let show' ~only_changed t =
  show
    (if only_changed
     then
       { t.t with
         ns_stats =
           let res = Hashtbl.create 3 in
           Hashtbl.iter
             (fun namespace stats ->
              if Hashtbl.mem t.changed_ns_stats namespace
              then Hashtbl.add res namespace stats)
             t.t.ns_stats;
           res
       }
     else t.t)

let clear_ns_stats_changed t =
  let r = Hashtbl.length t.changed_ns_stats in
  Hashtbl.clear t.changed_ns_stats;
  r

let new_upload t ns delta =
  let ns_stats = find t ns in
  ns_stats.upload <- Stat.update ns_stats.upload delta

let incr_manifest_src ns_stats =
  let open Cache in
  function
  | Fast ->
     ns_stats.manifest_cached <- ns_stats.manifest_cached + 1
  | Slow ->
     ns_stats.manifest_from_nsm <- ns_stats.manifest_from_nsm + 1
  | Stale ->
     ns_stats.manifest_stale <- ns_stats.manifest_stale + 1

let new_download t ns delta manifest_src (fg_hits, fg_misses) =
  let ns_stats = find t ns in
  let () =

    incr_manifest_src ns_stats manifest_src
  in
  let () = ns_stats.fragment_cache_hits <-
             ns_stats.fragment_cache_hits  + fg_hits
  in
  let () = ns_stats.fragment_cache_misses <-
             ns_stats.fragment_cache_misses + fg_misses
  in
  ns_stats.download <- Stat.update ns_stats.download delta

let new_read_objects_slices
      t ns
      ~total_length ~n_slices ~n_objects ~mf_sources
      ~fc_hits ~fc_misses
      ~took
  =
  let ns_stats = find t ns in
  ns_stats.partial_read_size    <- Stat.update ns_stats.partial_read_size  (float total_length);
  ns_stats.partial_read_count   <- Stat.update ns_stats.partial_read_count (float n_slices);
  ns_stats.partial_read_objects <- Stat.update ns_stats.partial_read_objects (float n_objects);
  ns_stats.partial_read_time    <- Stat.update ns_stats.partial_read_time  took;
  List.iter (incr_manifest_src ns_stats) mf_sources;
  ns_stats.fragment_cache_hits   <- ns_stats.fragment_cache_hits + fc_hits;
  ns_stats.fragment_cache_misses <- ns_stats.fragment_cache_misses + fc_misses;
  ()

