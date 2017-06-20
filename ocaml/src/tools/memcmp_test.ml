(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Memcmp

let test_compare () =
  assert (compare "abc" 0 3 "xabc" 1 3 = 0);
  assert (compare "abc" 0 3 "abcde" 0 5 = -1);
  assert (compare "abcde" 0 5 "abc" 0 3 =  1);
  assert (compare "" 0 0 "abc" 0 3 = -1);
  assert (compare "abcd" 0 4 "abcef" 0 5 = -1);
  assert (compare "abcdf" 0 5 "abce" 0 4 = -1);

  assert (compare' "abc" 0 3 (Lwt_bytes.of_string "abd") 0 3 = -1);
  assert (compare' "abc" 0 3 (Lwt_bytes.of_string "abb") 0 3 = 1)

open OUnit

let suite = "memcmp" >::: [
      "test_compare" >:: test_compare;
    ]
