(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Fragment_helper
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
        (Lwt_bytes.copy data) >>= fun (encrypted, fragment_ctr) ->

      Lwt_log.debug_f "data=%S encrypted=%S"
                      (Lwt_bytes.show data) (Lwt_bytes.show encrypted) >>= fun () ->

      maybe_decrypt
        encryption
        ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id:false
        ~fragment_ctr
        (Lwt_bytes.copy encrypted) >>= fun decrypted ->
      let decrypted = Bigstring_slice.extract_to_bigstring decrypted in

      Lwt_log.debug_f "decrypted=%S" (Lwt_bytes.show decrypted) >>= fun () ->

      assert (data = decrypted);

      Lwt.return ()
    in
    Lwt_list.iter_s
      t
      [ 0; 1; 15; 16; 17; 32; 255; 1024; 100_001; ]
  in
  Test_extra.lwt_run
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
