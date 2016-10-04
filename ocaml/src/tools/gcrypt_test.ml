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
open Gcrypt
open Lwt.Infix

let test_encrypt_decrypt () =
  let t () =
    let data = "abc" in
    let block_len = 16 in
    let key = String.make 32 'a' in
    let get_res () =
      let padded_data = Padding.pad (Bigstring_slice.of_string data) block_len in
      Cipher.with_t_lwt
        key Cipher.AES256 Cipher.CBC []
        (fun cipher ->
          Cipher.set_iv cipher (String.make block_len '\000');
          Cipher.encrypt cipher padded_data)
      >>= fun () ->
      Lwt.return padded_data
    in
    get_res () >>= fun res1 ->
    get_res () >>= fun res2 ->
    assert (res1 = res2);

    Cipher.with_t_lwt
      key Cipher.AES256 Cipher.CBC []
      (fun cipher ->
         Cipher.decrypt
           cipher
           res1
           0
           (Lwt_bytes.length res1)) >>= fun () ->
    let data' = Padding.unpad res1 in
    OUnit.assert_equal (Bigstring_slice.to_string data') data;
    Lwt.return ()
  in
  Lwt_main.run (t ())

let test_sha1 () =
  let open Digest in
  let hd = open_ SHA1 in
  write hd "abc";
  final hd;
  let res = Option.get_some (read hd SHA1) in
  assert ("a9993e364706816aba3e25717850c26c9cd0d89d" = to_hex res)

open OUnit

let suite = "gcrypt_test" >:::[
    "test_encrypt_decrypt" >:: test_encrypt_decrypt;
    "test_sha1" >:: test_sha1;
  ]
