(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Policy

let test_required_osds_per_node () =
  let osds =
    [ 0l, "1"; 1l, "1"; 2l,  "1"; 3l,  "1";
      4l, "2"; 5l, "2"; 7l,  "2"; 8l,  "2";
      8l, "3"; 9l, "3"; 10l, "3"; 11l, "3"; ]
  in
  assert (1 = required_osds_per_node osds 2);
  assert (1 = required_osds_per_node osds 3);
  assert (2 = required_osds_per_node osds 4);
  assert (2 = required_osds_per_node osds 5);
  assert (2 = required_osds_per_node osds 6);
  assert (3 = required_osds_per_node osds 7);
  assert (4 = required_osds_per_node osds 12);
  ()

open OUnit

let suite = "policy_test" >:::[
      "test_required_osds_per_node" >:: test_required_osds_per_node;
    ]
