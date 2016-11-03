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

let test_range () =
  assert ([ 0; 1; 2; 3; ] = Int.range 0 4)

let test_list_find_index () =
  assert (List.find_index ((=) 3) [] = None);
  assert (List.find_index ((=) 3) [ 0; 4; 6; ] = None);
  assert (List.find_index ((=) 3) [ 0; 3; 6; ] = Some 1);
  assert (List.find_index ((=) 3) [ 3; 6; 3; ] = Some 0)

let test_merge_head () =
  let _equal = OUnit.assert_equal ~printer:[%show : int list] in
  _equal ([0;1;2;3;4]) (List.merge_head [0;2;4;6;8] [1;3;5;7]    5) ;
  _equal ([0;1;2;3;4]) (List.merge_head [1;3;5;7]   [0;2;4;6;8]  5) ;
  _equal ([0;1;2;4;6]) (List.merge_head [0;2;4;6;8] [0;1]        5) ;
  _equal ([0;1;2;4;6]) (List.merge_head [0;1]       [0;2;4;6;8]  5)

let test_xint64 serialize_x_int64 deserialize_x_int64 =
  let edge = Int64.of_int32 Int32.max_int in
  let numbers = [ 0L; 2L; 1000L; Int64.max_int; ]
                |> List.append
                     (List.map
                        (fun i -> Int64.add edge i)
                        [ -3L; -1L; 0L; 1L; 45L; ])
  in
  List.iter
    (fun i ->
      serialize_x_int64 i
      |> deserialize_x_int64
      |> fun i' ->
         Printf.printf "%Li ?= %Li\n" i i';
         assert (i = i'))
    numbers

let test_xint64_le () =
  test_xint64 (serialize x_int64_to) (deserialize x_int64_from)

let test_xint64_be () =
  test_xint64 (serialize x_int64_be_to) (deserialize x_int64_be_from)

open OUnit

let suite = "prelude_test" >::: [
      "test_range" >:: test_range;
      "test_list_find_index" >:: test_list_find_index;
      "test_merge_head" >:: test_merge_head;
      "test_xint64_le" >:: test_xint64_le;
      "test_xint64_be" >:: test_xint64_be;
  ]
