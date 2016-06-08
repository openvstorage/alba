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
