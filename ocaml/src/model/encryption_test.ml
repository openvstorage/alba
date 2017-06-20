(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)


open! Prelude
open OUnit
open Encryption

let test_serialization () =

  let open Encryption in
  let key = "00000000000000000000000000000000" in
  let tests =
    [ NoEncryption;
      AlgoWithKey(AES(CTR,L256), key);
      AlgoWithKey(AES(CBC,L256), key);
    ]
  in
  List.iter
    (fun x ->
      let buffer = Buffer.create 128 in
      let () = Encryption.to_buffer buffer x in
      let s = Buffer.contents buffer in
      let () = Printf.printf "%s\n" (Prelude.to_hex s) in
      let buf' = Llio.make_buffer s 0 in
      let x' = Encryption.from_buffer buf' in
      OUnit.assert_equal ~printer:Encryption.show x x'
    ) tests



let suite = "encryption_test" >:::[
      "test_serialization" >:: test_serialization;
    ]
