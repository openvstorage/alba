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
