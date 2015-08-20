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

open Prelude
open Gcrypt
open Lwt.Infix

let test_encrypt_decrypt () =
  let t () =
    let data = "abc" in
    let block_len = 16 in
    let key = String.make 32 'a' in
    let get_res () =
      let padded_data = Padding.pad (Lwt_bytes.of_string data) block_len in
      Cipher.with_t_lwt
        key Cipher.AES256 Cipher.CBC []
        (fun cipher ->
           Cipher.encrypt
             ~iv:(String.make block_len '\000')
             cipher
             padded_data) >>= fun () ->
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
           res1) >>= fun () ->
    let data' = Padding.unpad ~release_input:false res1 in
    OUnit.assert_equal (Lwt_bytes.to_string data') data;
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
