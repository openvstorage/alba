(*
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

let required_osds_per_node osds desired_fragment_count =
  let rec inner node_osds osds_per_node possible_fragment_count =
    let node_osds, possible_fragment_count =
      List.fold_left
        (fun (acc, cnt) node_osds ->
         match node_osds with
         | [] -> acc, cnt
         | _ :: tl -> tl :: acc, cnt + 1)
        ([], possible_fragment_count)
        node_osds
    in
    if possible_fragment_count >= desired_fragment_count
    then osds_per_node
    else inner node_osds (osds_per_node + 1) possible_fragment_count
  in
  let node_osds =
    List.group_by
      (fun (osd_id, node_id) -> node_id)
      osds
    |> Hashtbl.to_assoc_list
    |> List.map snd
  in
  inner node_osds 1 0

let get_first_applicable_policy (policies : policy list) osds =
  List.find'
    (fun ((k, m, min_fragment_count, max_osds_per_node) as policy) ->
       (* TODO could memoize get_applicable_osd_count *)
       let cnt = get_applicable_osd_count max_osds_per_node osds in
       if min_fragment_count <= cnt
       then Some (policy, min cnt (k+m))
       else None)
    policies
