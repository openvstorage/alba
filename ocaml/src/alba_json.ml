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
open Encryption
open Albamgr_protocol.Protocol

module Osd = struct
  open Nsm_model
  type t = {
    id : Osd.id option;
    alba_id : string option;
    kind : string;

    (* for asd/kinetic *)
    ips : OsdInfo.ip list option;
    port : OsdInfo.port option;
    use_tls: bool option;
    use_rdma: bool option;

    (* for alba osd *)
    albamgr_cfg : Alba_arakoon.Config.t option;
    prefix : string option;
    preset : string option;

    (* for proxy osd *)
    endpoints : string list option;

    decommissioned : bool;
    node_id : OsdInfo.node_id;
    long_id : OsdInfo.long_id;
    total: int64;
    used : int64;
    seen : timestamp list;
    read : timestamp list;
    write : timestamp list;
    errors : (timestamp * string) list;
    checksum_errors : int64;
    claimed_since : timestamp option;
    }
  [@@deriving yojson]

  type t_list = t list [@@deriving yojson]

  let make
      alba_id claim_info
      { OsdInfo.kind; decommissioned; node_id;
        other;
        total; used;
        seen; read; write;
        errors; checksum_errors;
        claimed_since;
      } =
    let id, alba_id =
      let open Osd.ClaimInfo in
      match claim_info with
      | ThisAlba id -> Some id, Some alba_id
      | AnotherAlba alba -> None, Some alba
      | Available -> None, None
    in
    let kind, long_id,
        (ips, port, use_tls, use_rdma),
        (albamgr_cfg, prefix, preset),
        endpoints
      =
      let get_ips_port_tls_rdma (ips, port, use_tls, use_rdma) =
        Some ips, Some port, Some use_tls, Some use_rdma
      in
      let no_alba_osd = None, None, None in
      match kind with
      | OsdInfo.Asd (conn_info, asd_id) ->
         "AsdV1", asd_id,
         get_ips_port_tls_rdma conn_info,
         no_alba_osd,
         None
      | OsdInfo.Kinetic (conn_info, kin_id) ->
         "Kinetic3", kin_id,
         get_ips_port_tls_rdma conn_info,
         no_alba_osd,
         None
      | OsdInfo.Alba { OsdInfo.id; cfg; prefix; preset } ->
         "Alba", id,
         (None, None, None, None),
         (Some cfg, Some prefix, Some preset),
         None
      | OsdInfo.Alba2 { OsdInfo.id; cfg; prefix; preset } ->
         "Alba2", id,
         (None, None, None, None),
         (Some cfg, Some prefix, Some preset),
         None
      | OsdInfo.AlbaProxy { OsdInfo.endpoints; id; prefix; preset; } ->
         "AlbaProxy", id,
         (None, None, None, None),
         (None, Some prefix, Some preset),
         Some endpoints
    in
    { id; alba_id;
      kind;
      ips; port;
      use_tls; use_rdma;
      albamgr_cfg; prefix; preset;
      endpoints;
      node_id; long_id;
      decommissioned;
      total; used;
      seen; read; write; errors;
      checksum_errors;
      claimed_since;
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
        storage_per_osd : (int64 * int64) list;
        bucket_count : (Policy.policy * int64) list;
    }
    [@@deriving yojson]
  end

  module Both = struct
    type both = {
        name: string;
        statistics : Statistics.t;
        namespace  : t
      }
    [@@deriving yojson]
    type both_list = both list [@@deriving yojson]
    type c_both_list = int * both_list [@@deriving yojson]
  end
end

module AsdStatistics = struct
    type t = (Asd_statistics.AsdStatistics.t * (int64 * int64))
    let to_yojson (stats, disk_usage) =
      let open Asd_statistics.AsdStatistics in
      `Assoc (Hashtbl.fold
                (fun code (stat:Stat.Stat.stat) acc ->
                 (Asd_protocol.Protocol.code_to_description code, (* Slighty different from before *)
                  Stat.Stat.stat_to_yojson stat) :: acc
                )
                stats.G.statistics
                [
                  ("creation", `Float stats.G.creation);
                  ("period", `Float stats.G.period);
                  ("disk_usage", `Float (fst disk_usage |> Int64.to_float));
                  ("capacity", `Float (snd disk_usage |> Int64.to_float));
                ])

    let of_yojson _ = failwith "of_yojson: not implemented"
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
    match info.Nsm_host.kind with
    | Nsm_host.Arakoon cfg ->
       let open Alba_arakoon.Config in
       { id;
         kind = "arakoon";
         cluster_id = cfg.cluster_id;
         nodes =
           List.map
             (fun (name, { ips; port; }) -> { name; ips; port })
             cfg.node_cfgs;
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

  type osds = Preset.osds =
    | All      [@name "all"]
    | Explicit of int64 list [@name "explicit"]
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
    | AES_CBC_256 of string [@name "aes-cbc-256"]
    | AES_CTR_256 of string [@name "aes-ctr-256"]
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

  let to_yojson t =
    let j = to_yojson t in
    let extras =
      if t.in_use
      then [ ("in_use", `Bool true) ]
      else []
    in
    let extras =
      if t.is_default
      then extras
      else ("is_default", `Bool false) :: extras
    in
    match j with
    | `Assoc l -> `Assoc (List.rev_append extras l)
    | _ -> assert false

  type t_list = t list [@@deriving yojson]

  let make (name, preset, is_default, in_use) =
    let open Preset in
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
         | Bzip2 -> "bz2"
         | Test  -> "test"
        );
      fragment_checksum = preset.fragment_checksum_algo;
      object_checksum = preset.object_checksum;
      fragment_encryption =
        (let open Encryption in
         match preset.fragment_encryption with
         | AlgoWithKey (AES (CBC, L256), key) -> AES_CBC_256 (HexString.show key)
         | AlgoWithKey (AES (CTR, L256), key) -> AES_CTR_256 (HexString.show key)
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
    let open Preset in
    let open Lwt.Infix in

    (let open Encryption in
     match fragment_encryption with
     | NO_ENCRYPTION ->
       Lwt.return NoEncryption
     | AES_CBC_256 enc_key_file ->
       Lwt_extra2.read_file enc_key_file >>= fun enc_key ->
       Lwt_log.debug_f "Read encryption key with size %i" (Bytes.length enc_key) >>= fun () ->
       Lwt.return (AlgoWithKey (AES (CBC, L256), enc_key))
     | AES_CTR_256 enc_key_file ->
       Lwt_extra2.read_file enc_key_file >>= fun enc_key ->
       Lwt_log.debug_f "Read encryption key with size %i" (Bytes.length enc_key) >>= fun () ->
       Lwt.return (AlgoWithKey (AES (CTR, L256), enc_key)))
    >>= fun fragment_encryption ->

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
           | "test" -> Test
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
    safety_count : int64 option;
    bucket_safety : Disk_safety.bucket_safety list
  }
  [@@deriving yojson]

  type t_list = t list [@@deriving yojson]
end

module Version = struct
  type t = int * int * int * string [@@deriving yojson]
end

module Checksum = struct
  type t = Checksum.Checksum.t =
    | NoChecksum
    | Sha1 of HexString.t
    | Crc32c of HexInt32.t
    [@@deriving yojson]

end

module Progress = struct
  type t = Albamgr_protocol.Protocol.Progress.t [@@ deriving yojson]

  type progresses = (int * t) list [@@deriving yojson]
end

let string_list_to_json xs = `List (List.map (fun x -> `String x ) xs)

module Manifest = struct
  type t = Nsm_model.Manifest.t [@@ deriving yojson]
end
