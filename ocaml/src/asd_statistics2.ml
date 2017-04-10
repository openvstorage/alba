(*
Copyright (C) 2017 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*)

open Prelude

type bucket = (int, int) Hashtbl.t
let bucket_to_yojson b =
  `Assoc
   (Hashtbl.to_assoc_list b
    |> List.sort (fun (k1, _) (k2, _) -> compare k1 k2)
    |> List.map (fun (k, v) -> string_of_int k, `Int v)
   )
let bucket_of_yojson j = failwith "not implemented"

type stat = {
    creation_time : float;
    mutable latest_update : float;
    mutable n : int;
    mutable min_usec : int;
    mutable max_usec : int;
    mutable total_usec : int;
    buckets_usec_count : bucket;
  } [@@deriving yojson]

let make_stat () =
  let current_time = Unix.gettimeofday () in
  { creation_time = current_time;
    latest_update = current_time;
    n = 0;
    min_usec = max_int;
    max_usec = 0;
    total_usec = 0;
    buckets_usec_count = Hashtbl.create 3;
  }

let add_new_sample (t : stat) t0 t1 =
  let duration = t0 -. t1 in
  let duration_usec = (duration *. 1_000_000.) |> int_of_float in
  t.latest_update <- max t.latest_update t1;
  t.n <- t.n + 1;
  t.min_usec <- min t.min_usec duration_usec;
  t.max_usec <- min t.max_usec duration_usec;
  t.total_usec <- t.total_usec + duration_usec;

  let log2 x = (log x) /. (log 2.) in
  let duration_bucket =
    log2 (duration *. 1_000_000.)
    |> ceil |> int_of_float
    |> fun x -> 1 lsl x
  in
  let cnt =
    try Hashtbl.find t.buckets_usec_count duration_bucket
    with Not_found -> 0
  in
  Hashtbl.replace t.buckets_usec_count duration_bucket (cnt + 1);
  ()

type t = {
    partial_read_high_prio : stat;
    partial_read_low_prio : stat;
    store_fragment_high_prio : stat;
    store_fragment_low_prio : stat;
    delete_fragments_high_prio : stat;
    delete_fragments_low_prio : stat;
    delete_epoch_tag_low_prio : stat;
    other_applies_high_prio : stat;
    other_applies_low_prio : stat;
    range_entries_high_prio : stat;
    range_entries_low_prio : stat;
    range_high_prio : stat;
    range_low_prio : stat;
    range_validate_high_prio_verify_checksum : stat;
    range_validate_low_prio_verify_checksum : stat;
    range_validate_high_prio : stat;
    range_validate_low_prio : stat;
  } [@@deriving yojson]

let make_t () =
  { partial_read_high_prio = make_stat ();
    partial_read_low_prio = make_stat ();
    store_fragment_high_prio = make_stat ();
    store_fragment_low_prio = make_stat ();
    delete_fragments_high_prio = make_stat ();
    delete_fragments_low_prio = make_stat ();
    delete_epoch_tag_low_prio = make_stat ();
    other_applies_high_prio = make_stat ();
    other_applies_low_prio = make_stat ();
    range_entries_high_prio = make_stat ();
    range_entries_low_prio = make_stat ();
    range_high_prio = make_stat ();
    range_low_prio = make_stat ();
    range_validate_high_prio_verify_checksum = make_stat ();
    range_validate_low_prio_verify_checksum = make_stat ();
    range_validate_high_prio = make_stat ();
    range_validate_low_prio = make_stat ();
  }
