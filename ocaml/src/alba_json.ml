(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

open Prelude
open Encryption
open Albamgr_protocol.Protocol

let timestamp_to_yojson t =
  `String (show_timestamp t)
let timestamp_of_yojson = function
  | `String fs ->
    `Ok (float_of_string fs)
  | e ->
    `Error (Yojson.Safe.to_string e)

module Osd = struct
  type t = {
    id : Osd.id option;
    alba_id : string option;
    ips : Osd.ip list;
    port : Osd.port;
    node_id : Osd.node_id;
    kind : string;
    long_id : Osd.long_id;
    total: int64;
    used : int64;
    seen : timestamp list;
    read : timestamp list;
    write : timestamp list;
    errors : (timestamp * string) list;
    }
  [@@deriving yojson]

  type t_list = t list [@@deriving yojson]

  let make
      alba_id claim_info
      { Osd.kind; decommissioned; node_id;
        other;
        total; used;
        seen; read; write; errors; } =
    let id, alba_id =
      let open Osd.ClaimInfo in
      match claim_info with
      | ThisAlba id -> Some id, Some alba_id
      | AnotherAlba alba -> None, Some alba
      | Available -> None, None
    in
    let k, ips, port, long_id =
      match kind with
      | Osd.Asd (ips, port, asd_id)    -> "AsdV1", ips, port, asd_id
      | Osd.Kinetic(ips, port, kin_id) -> "Kinetic3", ips, port, kin_id
    in
    { id; alba_id;
      kind = k; ips; port; node_id; long_id;
      total; used;
      seen; read; write; errors;
    }
end

module Namespace = struct
  type t = {
    id : Namespace.id;
    name : Namespace.name;
    nsm_host_id : Nsm_host.id;
    state : string;
    preset_name : string;
  }
  [@@deriving yojson]

  type t_list = t list [@@deriving yojson]

  let make name { Namespace.id; nsm_host_id; state; preset_name; } =
    { id; name; nsm_host_id;
      state =
        (let open Namespace in
         match state with
         | Creating -> "creating"
         | Active -> "active"
         | Removing -> "removing"
         | Recovering -> "recovering");
      preset_name; }

  module Statistics = struct
    type t = {
        logical : int64;
        storage : int64;
        storage_per_osd : (int32 * int64) list;
        bucket_count : (Policy.policy * int64) list;
    }
    [@@deriving yojson]
  end
end

module AsdStatistics = struct
    type t = Asd_statistics.AsdStatistics.t [@@ deriving yojson]
    let make t = t
end

module ProxyStatistics = struct
    type t = Proxy_protocol.ProxyStatistics.t [@@ deriving yojson]
    let make t = t
end

module Nsm_host = struct
  type arakoon_node_cfg = {
    name : string;
    ips : string list;
    port : int;
  }
  [@@deriving yojson]

  type t = {
    id : Nsm_host.id;
    kind : string;
    cluster_id : string;
    nodes : arakoon_node_cfg list;
    lost : bool;
    namespaces_count : int;
  }
  [@@deriving yojson]

  type t_list = t list [@@deriving yojson]

  let make id info namespaces_count =
    let (cluster_id, nodes_hashtbl) = match info.Nsm_host.kind with
      | Nsm_host.Arakoon cfg -> cfg in
    { id; kind = "arakoon"; cluster_id;
      nodes =
        Hashtbl.fold
          (fun name { Arakoon_config.ips; port; } acc ->
             { name; ips; port; } :: acc)
          nodes_hashtbl
          [];
      lost = info.Nsm_host.lost;
      namespaces_count = Int64.to_int namespaces_count;
    }
end

module AlbaId = struct
  type t = {
    id : string;
  }
  [@@deriving yojson]

  let make id = { id; }
end

module ClaimedByResult = struct
  type t = {
    alba_id : string option
  }
  [@@deriving yojson]
end

module Preset = struct

  type osds = Albamgr_protocol.Protocol.Preset.osds =
    | All      [@name "all"]
    | Explicit of (int32 list) [@name "explicit"]
  [@@deriving yojson]

  type checksum_algo =
      Checksum.Checksum.Algo.t =
    | NO_CHECKSUM [@name "none"]
    | SHA1        [@name "sha-1"]
    | CRC32c      [@name "crc-32c"]
  [@@deriving yojson]

  type object_checksum =
      Preset.object_checksum = {
    allowed : checksum_algo list;
    default : checksum_algo;
    verify_upload : bool;
  }
  [@@deriving yojson]

  type fragment_encryption =
    | NO_ENCRYPTION [@name "none"]
    | AES_CBC_256   [@name "aes-cbc-256"] of string
  [@@deriving yojson]

  type t = {
    name : (string [@default ""]);
    policies : (int * int * int * int) list;
    is_default : (bool [@default false]);
    fragment_size : int;
    osds : osds;
    compression : string;
    fragment_checksum : checksum_algo;
    object_checksum : object_checksum;
    fragment_encryption : fragment_encryption;
    in_use : (bool [@default true]);
  } [@@deriving yojson]

  type t_list = t list [@@deriving yojson]

  let make (name, preset, is_default, in_use) =
    let open Albamgr_protocol.Protocol.Preset in
    { name;
      policies = preset.policies;
      is_default;
      fragment_size = preset.fragment_size;
      osds = (match preset.osds with
          | Explicit os -> Explicit os
          | All -> All);
      compression =
        (let open Alba_compression.Compression in
         match preset.compression with
         | NoCompression -> "none"
         | Snappy -> "snappy"
         | Bzip2 -> "bz2");
      fragment_checksum = preset.fragment_checksum_algo;
      object_checksum = preset.object_checksum;
      fragment_encryption =
        (let open Encryption in
         match preset.fragment_encryption with
         | AlgoWithKey (AES (CBC, L256), key) -> AES_CBC_256 (HexString.show key)
         | NoEncryption -> NO_ENCRYPTION);
      in_use;
    }

  let to_preset
    { name; policies; is_default;
      fragment_size; osds;
      compression; object_checksum;
      fragment_checksum; fragment_encryption;
    }
    =
    let open Albamgr_protocol.Protocol.Preset in
    let open Lwt.Infix in

    (let open Encryption in
     match fragment_encryption with
     | NO_ENCRYPTION ->
       Lwt.return NoEncryption
     | AES_CBC_256 enc_key_file ->
       Lwt_extra2.read_file enc_key_file >>= fun enc_key ->
       Lwt_log.debug_f "Read encryption key with size %i" (Bytes.length enc_key) >>= fun () ->
       Lwt.return (AlgoWithKey (AES (CBC, L256), enc_key))) >>= fun fragment_encryption ->

    Lwt.return
      { policies;
        fragment_size;
        w = Nsm_model.Encoding_scheme.W8;
        fragment_checksum_algo = fragment_checksum;
        compression =
          (let open Alba_compression.Compression in
           match compression with
           | "snappy" -> Snappy
           | "bz2" -> Bzip2
           | "none" -> NoCompression
           | s -> failwith (Printf.sprintf "unknown compressor: %S" s));
        object_checksum;
        osds;
        fragment_encryption;
      }
end

module Error = struct
  type t = {
    message : string;
  } [@@deriving yojson]
end

module Result = struct
  type 'a t = {
    success : bool;
    result : 'a;
  } [@@deriving yojson]
end

module DiskSafety = struct
  type t = {
    namespace : string;
    safety : int option;
  }
  [@@deriving yojson]

  type t_list = t list [@@deriving yojson]
end
