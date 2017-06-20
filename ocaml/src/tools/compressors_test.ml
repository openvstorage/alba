(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
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


let test_test () =
  let open Lwt.Infix in
  let open Alba_compression.Compression in
  let t =
    let data =
      "'in politics, stupidity is not a handicap' (Napoleon Bonaparte)"
    in
    let data_s = Bigstring_slice.of_string data in
    compress Test data_s >>= fun c ->
    let cs = Bigstring_slice.wrap_bigstring c in
    decompress Test cs >>= fun data_ba' ->
    let data' = Lwt_bytes.to_string data_ba' in
    assert (data = data');
    compress Test data_s >>= fun c' ->
    assert (c' <> c);
    Lwt.return_unit
  in
  Lwt_main.run t;;


open OUnit

let suite = "compressors_test" >:::[
    "test_snappy" >:: test_snappy;
    "test_bzip2" >:: test_bzip2;
    "test_bzip2'" >:: test_bzip2';
    "test_test"   >:: test_test;
  ]
