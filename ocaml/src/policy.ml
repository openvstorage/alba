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

open Prelude

type k = int [@@deriving show, yojson]
type m = int [@@deriving show, yojson]
type fragment_count = int [@@deriving show, yojson]
type max_osds_per_node = int [@@deriving show, yojson]

type policy = k * m * fragment_count * max_osds_per_node [@@deriving show, yojson]

let _version = 1
let to_buffer buf x =
  let ser =
    (Llio.tuple4_to
       Llio.int_to
       Llio.int_to
       Llio.int_to
       Llio.int_to)
  in
  Deser.versioned_to _version ser buf x

let from_buffer buf =
  let deser =
    (Llio.tuple4_from
         Llio.int_from
         Llio.int_from
         Llio.int_from
         Llio.int_from)
  in
  Deser.versioned_from _version deser buf

let get_applicable_osd_count max_osds_per_node osds =
  let node_osds =
    List.group_by
      (fun (osd_id, node_id) -> node_id)
      osds
  in
  Hashtbl.fold
    (fun node_id node_osds acc ->
       acc + min max_osds_per_node (List.length node_osds))
    node_osds
    0

let get_first_applicable_policy (policies : policy list) osds =
  List.find'
    (fun ((k, m, min_fragment_count, max_osds_per_node) as policy) ->
       (* TODO could memoize get_applicable_osd_count *)
       let cnt = get_applicable_osd_count max_osds_per_node osds in
       if min_fragment_count <= cnt
       then Some (policy, min cnt (k+m))
       else None)
    policies
