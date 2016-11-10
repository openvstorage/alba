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
open Fragment_helper
open Lwt_bytes2
open Lwt.Infix

let test_encrypt_decrypt () =
  let t encryption =
    let object_id = get_random_string 45 in
    let chunk_id = Random.int 6 in
    let fragment_id = Random.int 6 in
    let t data_length =
      Lwt_log.debug_f "test_encrypt_decrypt encryption=%s, length=%i"
                      (Encryption.Encryption.show encryption) data_length >>= fun () ->
      let data = Lwt_bytes.create_random data_length in
      maybe_encrypt
        encryption
        ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id:false
        (Lwt_bytes.copy data) >>= fun encrypted ->

      Lwt_log.debug_f "data=%S encrypted=%S"
                      (Lwt_bytes.show data) (Lwt_bytes.show encrypted) >>= fun () ->

      maybe_decrypt
        encryption
        ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id:false
        (Lwt_bytes.copy encrypted) >>= fun decrypted ->
      let decrypted = Bigstring_slice.extract_to_bigstring decrypted in

      assert (data = decrypted);

      Lwt.return ()
    in
    Lwt_list.iter_s
      t
      [ 0; 1; 15; 16; 17; 32; 255; 1024; 100_001; ]
  in
  Lwt_main.run
    begin
      let open Encryption.Encryption in
      Lwt_list.iter_s
        t
        [ NoEncryption;
          AlgoWithKey (AES (CBC, L256), get_random_string (key_length L256));
          AlgoWithKey (AES (CTR, L256), get_random_string (key_length L256));
        ]
    end

open OUnit

let suite = "fragment_helper_test" >:::[
      "test_encrypt_decrypt" >:: test_encrypt_decrypt;
    ]
