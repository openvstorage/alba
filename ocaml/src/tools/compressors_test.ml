(*
Copyright 2015 Open vStorage NV

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

open Compressors

let test_snappy () =
  let data = "aba" in
  let c = Snappy.compress_string data in
  let data' = Snappy.uncompress_string c in
  assert (data = data')

let test_bzip2 () =
  let data = "fsdjakviviviavjfjdkxcl" in
  let c = Bzip2.compress_ba_to_string (Lwt_bytes.of_string data) in
  Printf.printf "c len = %i\n" (String.length c);
  let data' = Bzip2.decompress_string c in
  Printf.printf "len = %i %i\n" (String.length data) (String.length data');
  assert (data = data')

let test_bzip2' () =
  let data = "aba" in
  let c = Bzip2.compress_string data in
  let data' = Bzip2.decompress_string c in
  assert (data = data')



open OUnit

let suite = "compressors_test" >:::[
    "test_snappy" >:: test_snappy;
    "test_bzip2" >:: test_bzip2;
    "test_bzip2'" >:: test_bzip2';
  ]
