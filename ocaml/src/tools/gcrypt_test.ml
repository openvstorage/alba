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

open! Prelude
open Gcrypt
open Lwt_bytes2
open Lwt.Infix

let test_encrypt_decrypt_cbc () =
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
         Cipher.decrypt_detached
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


let test_ctr_encryption () =
  (* small test to verify my understanding of CBC vs CTR encryption modes *)
  (* cbc encrypt ctr using all zeroes iv
   * should give same result as
   * ctr encrypting all zeroes with ctr=ctr
   *)
  let t () =
    let block_len = 16 in
    let key = get_random_string block_len in
    let ctr = get_random_string block_len in
    let open Cipher in

    let encrypted1 = Lwt_bytes.of_string ctr in
    with_t_lwt
      key AES256 CBC []
      (fun handle ->
        set_iv handle (String.make block_len '\000');
        encrypt handle encrypted1
      ) >>= fun () ->

    let encrypted2 = Lwt_bytes.create block_len in
    Lwt_bytes.fill encrypted2 0 block_len '\000';
    with_t_lwt
      key AES256 CTR []
      (fun handle ->
        set_ctr handle ctr;
        encrypt handle encrypted2
      ) >>= fun () ->

    assert (encrypted1 = encrypted2);
    Lwt.return ()
  in
  Lwt_main.run (t ())


let test_encrypt_decrypt_ctr () =
  let block_len = 16 in
  let t ctr =
    let data = Lwt_bytes.create_random 100_001 in
    let key = get_random_string block_len in
    let open Gcrypt in
    let open Cipher in
    let with_handle f = with_t_lwt key AES256 CTR [] f in
    let encrypted = Lwt_bytes.copy data in
    with_handle
      (fun handle ->
        let () = set_ctr handle ctr in
        encrypt handle encrypted) >>= fun () ->

    let decrypt data offset length =
      let result = Lwt_bytes.extract data offset length in
      with_handle
        (fun handle ->
          set_ctr_with_offset handle ctr offset;

          decrypt_detached
            handle
            result
            0 (Lwt_bytes.length result)
        ) >>= fun () ->
      Lwt.return result
    in
    decrypt encrypted 0 (Lwt_bytes.length encrypted) >>= fun decrypted ->
    assert (decrypted = data);

    let test_partial_decrypt offset length =
      Lwt_log.debug_f "test_partial_decrypt %i %i" offset length >>= fun () ->
      decrypt encrypted offset length >>= fun decrypted ->
      assert (decrypted = Lwt_bytes.extract data offset length);
      Lwt.return ()
    in

    Lwt_list.iter_s
      (fun (offset, length) -> test_partial_decrypt offset length)
      [ (0, 256); (0, 1024); (1024, 256);
        (1025, 256); (1, 1); (1027, 6);
        (10_000, 4096); (100_000, 1);
      ] >>= fun () ->


    Lwt.return ()
  in
  Lwt_main.run
    begin
      let make_ctr cntr_high cntr_low =
        let ctr = String.make block_len 'a' in
        EndianBytes.BigEndian.set_int64 ctr 0 cntr_high;
        EndianBytes.BigEndian.set_int64 ctr 8 cntr_low;
        ctr
      in
      Lwt_list.iter_s
        (fun ctr ->
          Lwt_log.debug_f "ctr = %S" ctr >>= fun () ->
          t ctr)
        [ get_random_string block_len;
          get_random_string block_len;
          get_random_string block_len;
          get_random_string block_len;
          String.make block_len '\000';
          String.make block_len '\255';
          String.make block_len '\127';
          String.make block_len '\128';
          make_ctr Int64.max_int Int64.max_int;
          make_ctr (-1L)         Int64.max_int;
          make_ctr Int64.max_int (-1L);
          make_ctr (-1L)         (-1L);
        ]
    end

open OUnit

let suite = "gcrypt_test" >:::[
    "test_encrypt_decrypt_cbc" >:: test_encrypt_decrypt_cbc;
    "test_sha1" >:: test_sha1;
    "test_ctr_encryption" >:: test_ctr_encryption;
    "test_encrypt_decrypt_ctr" >:: test_encrypt_decrypt_ctr;
  ]
