(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open OUnit
open Cache

let assert_consistency cache =
  let open Cache in
  let via_next = Cache.order_next cache in
  let via_prev = Cache.order_prev cache in
  OUnit.assert_equal via_next via_prev;
  OUnit.assert_equal (List.length via_next)
                     (Hashtbl.length cache.h);
  ()

let test_consistency () =
  let c0 = Cache.make  ~max_size:2 (-1) in
  let () = Cache.add c0 2 "2" in
  let () = Cache.add c0 4 "4" in
  assert_consistency c0;
  let v4 = Cache.lookup c0 4 in
  OUnit.assert_equal v4 (Some "4");
  assert_consistency c0;
  let v2 = Cache.lookup c0 2 in
  OUnit.assert_equal v2 (Some "2");
  let () = Cache.add c0 3 "3" in
  OUnit.assert_equal (Cache.order_next c0) [3;2]

let test_weights () =
  let c0 = Cache.make ~max_size:10 (-1) ~weight:String.length in
  let () = Cache.add c0 2 "two" in
  let () = Cache.add c0 4 "four" in
  let () = Cache.add c0 5 "five" in
  assert_consistency c0;
  OUnit.assert_equal ~printer:[%show : int list] (Cache.order_next c0) [5;4];
  let v4 = Cache.lookup c0 4 in
  OUnit.assert_equal v4 (Some "four");
  assert_consistency c0;
  OUnit.assert_equal (Cache.order_next c0) [4;5]

let suite =[
    "consistency" >:: test_consistency;
    "weights" >:: test_weights;
  ]
