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

open Prelude
open Slice
open Nsm_model

module RecoveryInfo = struct
  type object_info = {
    storage_scheme : Storage_scheme.t;
    size : Int64.t;
    checksum : Checksum.t;
    timestamp : float;
  }

  type t' = {
    name : string;

    (* chunk specific info *)
    chunk_size : int;
    fragment_sizes : int list;
    fragment_checksums : Checksum.t list;

     (* only fill this in for the last chunk *)
    object_info_o : object_info option;
  }

  type t = {
    encrypt_info : EncryptInfo.t;
    (* compressed + maybe_encrypted t' *)
    payload : string;
  }

  let object_info_to_buffer buf t =
    Llio.int8_to buf 1;
    Storage_scheme.output buf t.storage_scheme;
    Llio.int64_to buf t.size;
    Checksum.output buf t.checksum;
    Llio.float_to buf t.timestamp

  let object_info_from_buffer buf =
    let tag = Llio.int8_from buf in
    if tag <> 1
    then raise_bad_tag "RecoveryInfo.object_info" tag;

    let storage_scheme = Storage_scheme.input buf in
    let size = Llio.int64_from buf in
    let checksum = Checksum.input buf in
    let timestamp = Llio.float_from buf in
    { storage_scheme; size; checksum; timestamp; }

  (* TODO store it on osd with a checksum? ->
     yes, and osds should be able to return/verify the checksum *)

  let to_buffer' buf t =
    Llio.string_to buf t.name;
    Llio.int_to buf t.chunk_size;
    Llio.list_to Llio.int_to buf t.fragment_sizes;
    Llio.list_to Checksum.output buf t.fragment_checksums;
    Llio.option_to object_info_to_buffer buf t.object_info_o

  let from_buffer' buf =
    let name = Llio.string_from buf in
    let chunk_size = Llio.int_from buf in
    let fragment_sizes = Llio.list_from Llio.int_from buf in
    let fragment_checksums = Llio.list_from Checksum.input buf in
    let object_info_o = Llio.option_from object_info_from_buffer buf in
    { name;
      chunk_size;
      fragment_sizes;
      fragment_checksums;
      object_info_o; }

  let to_buffer buf t =
    Llio.int8_to buf 1;
    EncryptInfo.to_buffer buf t.encrypt_info;
    Llio.string_to buf t.payload

  let from_buffer buf =
    let tag = Llio.int8_from buf in
    if tag <> 1
    then raise_bad_tag "RecoveryInfo.t" tag;

    let encrypt_info = EncryptInfo.from_buffer buf in
    let payload = Llio.string_from buf in
    { encrypt_info; payload; }

  let t'_to_t t' encryption ~object_id ~namespace =
    let open Lwt.Infix in
    serialize to_buffer' t'
    |> Compressors.Bzip2.compress_string_to_ba
    |> Bigstring_slice.wrap_bigstring
    |> Fragment_helper.maybe_encrypt
         encryption
         ~namespace
         ~object_id
         ~chunk_id:(-1) ~fragment_id:(-1) ~ignore_fragment_id:false
    >>= fun encrypted ->
    let payload = Bigstring_slice.to_string encrypted in
    EncryptInfo.from_encryption namespace encryption
    >>= fun encrypt_info ->
    Lwt.return { payload; encrypt_info; }

  let t_to_t' t encryption ~object_id ~namespace =
    let open Lwt.Infix in

    EncryptInfo.from_encryption namespace encryption
    >>= fun encrypt_info' ->
    assert (encrypt_info' = t.encrypt_info);

    Fragment_helper.maybe_decrypt
      encryption
      ~namespace
      ~object_id
      ~chunk_id:(-1) ~fragment_id:(-1) ~ignore_fragment_id:false
      (Lwt_bytes.of_string t.payload) >>= fun decrypted ->
    let t' =
      Compressors.Bzip2.decompress_bs_string decrypted |>
      deserialize from_buffer'
    in
    Lwt.return t'

  let make
        ~namespace
        ~object_name ~object_id
        object_info_o
        encryption
        chunk_size
        fragment_sizes
        fragment_checksums =
    let open Lwt.Infix in
    (t'_to_t
       { name = object_name;
         chunk_size;
         fragment_sizes;
         fragment_checksums;
         object_info_o; }
       encryption
       ~namespace
       ~object_id) >>= fun recov_info ->
    let recovery_info_slice =
      serialize
        to_buffer
        recov_info |>
      Slice.wrap_string
    in
    Lwt.return recovery_info_slice

end
