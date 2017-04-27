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
open Slice
open Nsm_model
open Lwt_bytes2

module RecoveryInfo = struct
  type object_info = {
    storage_scheme : Storage_scheme.t;
    size : Int64.t;
    checksum : Checksum.t;
    timestamp : float;
  } [@@ deriving show]

  type t' = {
    name : string;

    (* chunk specific info *)
    chunk_size : int;
    fragment_sizes : int list;
    fragment_checksums : Checksum.t list;

    fragment_ctr : string option;

     (* only fill this in for the last chunk *)
    object_info_o : object_info option;
  } [@@ deriving show]

  type t = {
    encrypt_info : EncryptInfo.t;
    (* compressed + maybe_encrypted t' *)
    payload : string;
    payload_ctr : string option;
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
    Llio.option_to object_info_to_buffer buf t.object_info_o;
    Llio.option_to Llio.string_to buf t.fragment_ctr

  let from_buffer' buf =
    let name = Llio.string_from buf in
    let chunk_size = Llio.int_from buf in
    let fragment_sizes = Llio.list_from Llio.int_from buf in
    let fragment_checksums = Llio.list_from Checksum.input buf in
    let object_info_o = Llio.option_from object_info_from_buffer buf in
    let fragment_ctr = maybe_from_buffer (Llio.option_from Llio.string_from) None buf in
    { name;
      chunk_size;
      fragment_sizes;
      fragment_checksums;
      fragment_ctr;
      object_info_o; }

  let to_buffer buf t =
    Llio.int8_to buf 1;
    EncryptInfo.to_buffer buf t.encrypt_info;
    Llio.string_to buf t.payload;
    Llio.option_to Llio.string_to buf t.payload_ctr

  let from_buffer buf =
    let tag = Llio.int8_from buf in
    if tag <> 1
    then raise_bad_tag "RecoveryInfo.t" tag;

    let encrypt_info = EncryptInfo.from_buffer buf in
    let payload = Llio.string_from buf in
    let payload_ctr = maybe_from_buffer (Llio.option_from Llio.string_from) None buf in
    { encrypt_info;
      payload;
      payload_ctr; }

  let t'_to_t t' encryption ~object_id =
    let open Lwt.Infix in
    serialize to_buffer' t'
    |> Compressors.Bzip2.compress_string_to_ba
    |> Fragment_helper.maybe_encrypt
         encryption
         ~object_id
         ~chunk_id:(-1) ~fragment_id:(-1) ~ignore_fragment_id:false
    >>= fun (encrypted, payload_ctr) ->
    let payload = Lwt_bytes.to_string encrypted in
    let () = Lwt_bytes.unsafe_destroy ~msg:"destroy encrypted" encrypted in
    let encrypt_info = Encrypt_info_helper.from_encryption encryption in

    Lwt.return { payload; payload_ctr; encrypt_info; }

  let t_to_t' t encryption ~object_id =
    let open Lwt.Infix in

    let encrypt_info' = Encrypt_info_helper.from_encryption encryption in
    assert (encrypt_info' = t.encrypt_info);

    Fragment_helper.maybe_decrypt
      encryption
      ~object_id
      ~chunk_id:(-1) ~fragment_id:(-1) ~ignore_fragment_id:false
      ~fragment_ctr:t.payload_ctr
      (Lwt_bytes.of_string t.payload) >>= fun decrypted ->
    let t' =
      Compressors.Bzip2.decompress_bs_string decrypted |>
      deserialize from_buffer'
    in
    Lwt.return t'

  let make
        ~object_name
        ~object_id
        object_info_o
        encryption
        chunk_size
        fragment_sizes
        fragment_checksums
        fragment_ctr
    =
    let open Lwt.Infix in
    (t'_to_t
       { name = object_name;
         chunk_size;
         fragment_sizes;
         fragment_checksums;
         fragment_ctr;
         object_info_o; }
       encryption
       ~object_id) >>= fun recov_info ->
    let recovery_info_slice =
      serialize
        to_buffer
        recov_info |>
      Slice.wrap_string
    in
    Lwt.return recovery_info_slice

end
