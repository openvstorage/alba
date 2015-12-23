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

(* This module exists so no 'nocrypto' stuff is used
 * inside the arakoon plugins. *)

let get_id_for_key key =
  let id =
    Cstruct.to_string
      (Nocrypto.Hash.SHA256.digest
         (Cstruct.of_string key))
  in
  Nsm_model.EncryptInfo.KeySha1 id

let from_encryption =
  let open Encryption in
  function
  | Encryption.NoEncryption ->
     Nsm_model.EncryptInfo.NoEncryption
  | Encryption.AlgoWithKey (algo, key) ->
     Nsm_model.EncryptInfo.Encrypted (algo, get_id_for_key key)

let get_encryption t encrypt_info =
  let open Nsm_model in
  let open Encryption.Encryption in
  let open Albamgr_protocol.Protocol in
  match t.Preset.fragment_encryption, encrypt_info with
  | NoEncryption, EncryptInfo.NoEncryption ->
     t.Preset.fragment_encryption
  | AlgoWithKey (algo, key), EncryptInfo.Encrypted (algo', id) ->
     if algo = algo'
     then begin
         let id' = get_id_for_key key in
         if id = id'
         then t.Preset.fragment_encryption
         else failwith "encrypted with another key"
       end else failwith "algo mismatch for decryption"
  | NoEncryption, EncryptInfo.Encrypted _
  | AlgoWithKey _, EncryptInfo.NoEncryption ->
     failwith "encryption & enc_info mismatch during decryption"
