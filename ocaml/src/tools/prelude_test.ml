(*
Copyright 2015 iNuron NV

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

let test_hashtbl_to_yojson () =
  let l = [ 1,2; 3,4; 5,6; ] in
  let h = Hashtbl.from_assoc_list l in
  begin
    let l' = Hashtbl.to_assoc_list h in
    let h' = Hashtbl.from_assoc_list l' in
    assert (h = h')
  end;
  let j = Hashtbl.to_yojson [%to_yojson : int] [%to_yojson : int] h in
  let h' =
    match Hashtbl.of_yojson [%of_yojson : int] [%of_yojson : int] j with
    | `Error e -> failwith e
    | `Ok v -> v
  in
  assert (h = h')

open OUnit

let suite = "prelude_test" >::: [
      "test_range" >:: test_range;
      "test_list_find_index" >:: test_list_find_index;
      "test_merge_head" >:: test_merge_head;
      "test_hashtbl_to_yojson" >:: test_hashtbl_to_yojson;
  ]
