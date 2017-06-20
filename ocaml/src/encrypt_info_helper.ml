(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

(* This module exists so no ctypes stuff is used
 * inside the arakoon plugins. *)

open! Prelude

let get_id_for_key key =
  let id =
    let open Gcrypt.Digest in
    let hd = open_ SHA256 in
    write hd key;
    final hd;
    let res = Option.get_some (read hd SHA256) in
    close hd;
    res
  in
  Nsm_model.EncryptInfo.KeySha256 id

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
