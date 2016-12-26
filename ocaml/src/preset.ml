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

type k = Policy.k [@@deriving show]
type m = Policy.m [@@deriving show]

module Encoding_scheme = struct
  type w =
    | W8 [@value 1]
  [@@deriving show, enum]

  let w_as_int = function
    | W8 -> 8

  let w_to_buffer buf w =
    Llio.int8_to buf (w_to_enum w)
  let w_from_buffer buf =
    let w_i = Llio.int8_from buf in
    match w_of_enum w_i with
    | None -> raise_bad_tag "Encoding_scheme.w" w_i
    | Some w -> w

  type t =
    | RSVM of k * m * w (* k identity blocks + m redundancy blocks * word size *)
  [@@deriving show]

  let output buf = function
    | RSVM (k, m, w) ->
      Llio.int8_to buf 1;
      Llio.int_to buf k;
      Llio.int_to buf m;
      w_to_buffer buf w

  let input buf =
    match Llio.int8_from buf with
    | 1 ->
      let k = Llio.int_from buf in
      let m = Llio.int_from buf in
      let w = w_from_buffer buf in
      RSVM (k, m, w)
    | k -> raise_bad_tag "Encoding_scheme" k
end

type osd_id = int64 [@@deriving show, yojson]

type osds =
  | All
  | Explicit of osd_id list
 [@@deriving show]

let osds_to_buffer buf = function
  | All -> Llio.int8_to buf 1
  | Explicit osd_ids ->
     Llio.int8_to buf 2;
     Llio.list_to x_int64_to buf osd_ids

let osds_from_buffer buf =
  match Llio.int8_from buf with
  | 1 -> All
  | 2 -> Explicit (Llio.list_from x_int64_from buf)
  | k -> raise_bad_tag "Preset.osds" k

type checksum_algo = Checksum.Checksum.algo [@@deriving show]

type object_checksum = {
    allowed : checksum_algo list;
    default : checksum_algo;
    verify_upload : bool;
  }
                         [@@deriving show]

let object_checksum_to_buffer buf t =
  let open Checksum.Checksum.Algo in
  to_buffer buf t.default;
  Llio.list_to to_buffer buf t.allowed;
  Llio.bool_to buf t.verify_upload

let object_checksum_from_buffer buf =
  let open Checksum.Checksum.Algo in
  let default = from_buffer buf in
  let allowed = Llio.list_from from_buffer buf in
  let verify_upload = Llio.bool_from buf in
  { allowed; default; verify_upload; }

type t = {
    w : Encoding_scheme.w;
    policies : Policy.policy list;
    fragment_size : int;
    osds : osds;
    compression : Alba_compression.Compression.t;
    object_checksum : object_checksum;
    fragment_checksum_algo : Checksum.Checksum.algo;
    fragment_encryption : Encryption.Encryption.t;
  }
           [@@deriving show]

type version = int64 [@@deriving show]

let is_valid t =
  let default_in_allowed_list = List.mem t.object_checksum.default t.object_checksum.allowed in
  let enc_key_length = Encryption.Encryption.is_valid t.fragment_encryption in
  let policies_ok =
    t.policies <> [] &&
      List.for_all
        (fun (k, m, fragment_count, _) ->
          (k > 0)
          && (k <= fragment_count)
          && (fragment_count <= k + m)
        )
        t.policies
  in
  let fragment_size_ok = t.fragment_size >= Fragment_size_helper.fragment_multiple in
  default_in_allowed_list
  && enc_key_length
  && policies_ok
  && fragment_size_ok

type name = string [@@deriving show]

let _to_buffer buf t =
  Encoding_scheme.w_to_buffer buf t.w;
  Llio.list_to
    Policy.to_buffer
    buf
    t.policies;
  Llio.int_to buf t.fragment_size;
  osds_to_buffer buf t.osds;
  Alba_compression.Compression.output buf t.compression;
  object_checksum_to_buffer buf t.object_checksum;
  Checksum.Checksum.Algo.to_buffer buf t.fragment_checksum_algo;
  Encryption.Encryption.to_buffer buf t.fragment_encryption

let to_buffer ~version buf t =
  Llio.int8_to buf version;
  match version with
  | 1 -> _to_buffer buf t
  | 2 ->
     let s = serialize _to_buffer t in
     Llio.string_to buf s
  | k ->
     assert false

let _from_buffer buf =
  let w = Encoding_scheme.w_from_buffer buf in
  let policies =
    Llio.list_from
      Policy.from_buffer
      buf in
  let fragment_size = Llio.int_from buf in
  let osds = osds_from_buffer buf in
  let compression = Alba_compression.Compression.input buf in
  let object_checksum = object_checksum_from_buffer buf in
  let fragment_checksum_algo = Checksum.Checksum.Algo.from_buffer buf in
  let fragment_encryption = Encryption.Encryption.from_buffer buf in
  { w; policies;
    fragment_size; osds; compression;
    object_checksum; fragment_checksum_algo;
    fragment_encryption; }

let from_buffer buf =
  let version = Llio.int8_from buf in
  match version with
  | 1 -> _from_buffer buf
  | 2 ->
     let s = Llio.string_from buf in
     deserialize _from_buffer s
  | k ->
     raise_bad_tag "Preset" k

let _DEFAULT = {
    policies = [(5, 4, 8, 3); (2, 2, 3, 4);];
    w = Encoding_scheme.W8;
    fragment_size = 1024 * 1024;
    osds = All;
    compression = Alba_compression.Compression.Snappy;
    object_checksum =
      (let open Checksum.Checksum.Algo in
       { allowed = [ NO_CHECKSUM;
                     SHA1;
                     CRC32c ];
         default = CRC32c;
         verify_upload = true;
      });
    fragment_checksum_algo = Checksum.Checksum.Algo.CRC32c;
    fragment_encryption = Encryption.Encryption.NoEncryption;
  }

module Update = struct
  type t = {
      policies' : (Policy.policy list option [@default None]) [@key "policies"];
    } [@@deriving show, yojson]

  let make ?policies' () = { policies'; }

  let apply preset t =
    { preset with
      policies = (Option.get_some_default
                    preset.policies
                    t.policies'); }

  let from_buffer buf =
    let ser_version = Llio.int8_from buf in
    assert (ser_version = 1);
    let policies' =
      Llio.option_from
        (Llio.list_from
           Policy.from_buffer)
        buf
    in
    { policies' }

  let to_buffer buf t =
    let ser_version = 1 in
    Llio.int8_to buf ser_version;
    Llio.option_to
      (Llio.list_to Policy.to_buffer)
      buf
      t.policies'
end

module Propagation = struct
  type namespace_id = int64 [@@deriving show]
  type t = version * namespace_id list [@@deriving show]

  let to_buffer buf (version, namespace_ids) =
    let ser_version = 1 in
    Llio.int8_to buf ser_version;
    Llio.int64_to buf version;
    Llio.list_to Llio.int64_to buf namespace_ids

  let from_buffer buf =
    let ser_version = Llio.int8_from buf in
    assert (ser_version = 1);
    let version = Llio.int64_from buf in
    let namespace_ids = Llio.list_from Llio.int64_from buf in
    (version, namespace_ids)
end
