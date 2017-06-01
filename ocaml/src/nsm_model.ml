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

open! Prelude
open Key_value_store

type object_name = string [@@deriving show]
type object_id = HexString.t [@@deriving show]


module OsdInfo = struct
  type long_id = string [@@deriving show, yojson]

  type ip = string [@@deriving show, yojson]
  type port = int [@@deriving show, yojson]

  type endpoint_uri = string [@@deriving show, yojson]

  type host = string
  type endpoint = Net_fd.transport * host * port

  let parse_endpoint_uri s =
    let uri = Uri.of_string s in
    let transport = match Uri.scheme uri with
      | None -> failwith "Transport should be 'tcp' or 'rdma'"
      | Some "tcp" -> Net_fd.TCP
      | Some "rdma" -> Net_fd.RDMA
      | Some s -> failwith (Printf.sprintf "Transport '%s' not recognized, should be 'tcp' or 'rdma'" s)
    in
    let host = Uri.host uri |> Option.get_some in
    let port = Uri.port uri |> Option.get_some in
    (transport, host, port)

  type node_id = string [@@deriving show, yojson]

  type asd_id = string [@@deriving show, yojson]
  type kinetic_id = string [@@ deriving show, yojson]
  type alba_id = string [@@ deriving show, yojson]

  type use_tls  = bool [@@ deriving show, yojson]
  type use_rdma = bool [@@ deriving show, yojson]
  type conn_info = ip list * port * use_tls * use_rdma [@@ deriving show, yojson]

  type alba_cfg = {
      cfg : Alba_arakoon.Config.t;
      id  : alba_id;
      prefix : string;
      preset : string;
    } [@@deriving show, yojson]

  type alba_proxy_cfg = {
      endpoints : endpoint_uri list;
      id : alba_id;
      prefix : string;
      preset : string;
    } [@@deriving show, yojson]

  type kind =
    | Asd     of conn_info * asd_id
    | Kinetic of conn_info * kinetic_id
    | Alba    of alba_cfg
    | Alba2   of alba_cfg
    | AlbaProxy of alba_proxy_cfg
                   [@@deriving show, yojson]

  let get_long_id = function
    | Asd (_, asd_id)         -> asd_id
    | Kinetic (_, kinetic_id) -> kinetic_id
    | Alba  { id; }
    | Alba2 { id; }
    | AlbaProxy { id; }     -> id

  type t = {
    kind : kind;
    decommissioned : bool;
    node_id : node_id;
    other : string;
    total : int64;
    used: int64;
    seen : timestamp list;
    read : timestamp list;
    write : timestamp list;
    errors : (timestamp * string) list;
    checksum_errors : int64;
    claimed_since : timestamp option;
  }
  [@@deriving show, yojson]

  let make
      ~kind ~node_id
      ~decommissioned ~other
      ~total ~used
      ~seen ~read ~write ~errors
      ~checksum_errors
      ~claimed_since
    =
    { kind; node_id;
      decommissioned; other;
      total; used;
      seen; read; write; errors;
      checksum_errors;
      claimed_since;
    }

  let _check_rdma = function
    | true ->
       let () = Lwt_log.ign_fatal "use_rdma & old serialization ?!" in
       failwith "use_rdma"
    | false -> ()

  let kind_to conn_info_to buf = function
    | Asd (conn_info, asd_id) ->
       Llio.int8_to buf 1;
       conn_info_to buf conn_info;
       Llio.string_to buf asd_id
    | Kinetic(conn_info, kin_id) ->
       Llio.int8_to buf 2;
       conn_info_to buf conn_info;
       Llio.string_to buf kin_id
    | Alba { cfg; id; prefix; preset; } ->
       Llio.int8_to buf 3;
       Alba_arakoon.Config.to_buffer buf cfg;
       Llio.string_to buf id;
       Llio.string_to buf prefix;
       Llio.string_to buf preset
    | Alba2 { cfg; id; prefix; preset; } ->
       Llio.int8_to buf 4;
       Alba_arakoon.Config.to_buffer buf cfg;
       Llio.string_to buf id;
       Llio.string_to buf prefix;
       Llio.string_to buf preset
    | AlbaProxy { endpoints; id; prefix; preset; } ->
       Llio.int8_to buf 5;
       Llio.list_to Llio.string_to buf endpoints;
       Llio.string_to buf id;
       Llio.string_to buf prefix;
       Llio.string_to buf preset

  let _to_buffer_1
      ?(ignore_tls = false)
      buf
      { kind; node_id;
        decommissioned; other;
        total; used;
        seen; read; write; errors; } =

    let ser_version = 1 in

    Llio.int8_to buf ser_version;
    let conn_info_to buf (ips,port,use_tls,use_rdma) =
      if use_tls && not ignore_tls
      then
        begin
        let () = Lwt_log.ign_fatal "use_tls ?!" in
        failwith "use_tls?"
        end;
      Llio.list_to Llio.string_to buf ips;
      Llio.int_to buf port;
      _check_rdma use_rdma

    in
    kind_to conn_info_to buf kind;
    Llio.string_to buf node_id;
    Llio.bool_to buf decommissioned;
    Llio.string_to buf other;
    Llio.int64_to buf total;
    Llio.int64_to buf used;
    Llio.list_to Llio.float_to buf seen;
    Llio.list_to Llio.float_to buf read;
    Llio.list_to Llio.float_to buf write;
    Llio.list_to
      (Llio.pair_to
         Llio.float_to
         Llio.string_to)
      buf
      errors

  let _to_buffer_2 final_buf
      { kind; node_id;
        decommissioned; other;
        total; used;
        seen; read; write; errors;
      }
    =
    let ser_version = 2 in
    Llio.int8_to final_buf ser_version;
    let conn_info_to buf (ips,port,use_tls, use_rdma) =
      Llio.list_to Llio.string_to buf ips;
      Llio.int_to buf port;
      Llio.bool_to buf use_tls;
      _check_rdma use_rdma
    in
    let buf = Buffer.create 128 in
    let () =
      kind_to conn_info_to buf kind;
      Llio.string_to buf node_id;
      Llio.bool_to buf decommissioned;
      Llio.string_to buf other;
      Llio.int64_to buf total;
      Llio.int64_to buf used;
      Llio.list_to Llio.float_to buf seen;
      Llio.list_to Llio.float_to buf read;
      Llio.list_to Llio.float_to buf write;
      Llio.list_to
        (Llio.pair_to
           Llio.float_to
           Llio.string_to)
        buf
        errors
    in
    Llio.string_to final_buf (Buffer.contents buf)

  let _to_buffer_3 final_buf
      { kind; node_id;
        decommissioned; other;
        total; used;
        seen; read; write; errors;
        checksum_errors;
        claimed_since
      }
    =
    let ser_version = 3 in
    Llio.int8_to final_buf ser_version;
    let conn_info_to buf (ips,port,use_tls, use_rdma) =
      Llio.list_to Llio.string_to buf ips;
      Llio.int_to buf port;
      Llio.bool_to buf use_tls;
      Llio.bool_to buf use_rdma
    in
    let buf = Buffer.create 128 in
    let () =
      kind_to conn_info_to buf kind;
      Llio.string_to buf node_id;
      Llio.bool_to buf decommissioned;
      Llio.string_to buf other;
      Llio.int64_to buf total;
      Llio.int64_to buf used;
      Llio.list_to Llio.float_to buf seen;
      Llio.list_to Llio.float_to buf read;
      Llio.list_to Llio.float_to buf write;
      Llio.list_to
        (Llio.pair_to
           Llio.float_to
           Llio.string_to)
        buf
        errors;
      Llio.int64_to buf checksum_errors;
      Llio.option_to Llio.float_to buf claimed_since

    in
    Llio.string_to final_buf (Buffer.contents buf)

  let to_buffer  buf t ~version =
    Lwt_log.ign_debug_f "OsdInfo.to_buffer ... ~version:%i" version;
    match version with
    | 3 -> _to_buffer_3 buf t
    | 2 -> _to_buffer_2 buf t
    | 1 -> _to_buffer_1 buf t
    | k -> raise_bad_tag "OsdInfo" k

  let _from_buffer1 buf =
    let kind = match Llio.int8_from buf with
      | 1 ->
        let ips = Llio.list_from Llio.string_from buf in
        let port = Llio.int_from buf in
        let asd_id = Llio.string_from buf in
        Asd ((ips, port,false,false ), asd_id)
      | 2 ->
        let ips = Llio.list_from Llio.string_from buf in
        let port = Llio.int_from buf in
        let kin_id = Llio.string_from buf in
        Kinetic((ips, port, false, false), kin_id)
      | 3 ->
        let cfg = Alba_arakoon.Config.from_buffer buf in
        let id = Llio.string_from buf in
        let prefix = Llio.string_from buf in
        let preset = Llio.string_from buf in
        Alba { cfg; id; prefix; preset; }
      | 4 ->
        let cfg = Alba_arakoon.Config.from_buffer buf in
        let id = Llio.string_from buf in
        let prefix = Llio.string_from buf in
        let preset = Llio.string_from buf in
        Alba2 { cfg; id; prefix; preset; }
      | k -> raise_bad_tag "OsdInfo" k in
    let node_id = Llio.string_from buf in
    let decommissioned = Llio.bool_from buf in
    let other  = Llio.string_from buf in
    let total  = Llio.int64_from buf in
    let used   = Llio.int64_from buf in
    let seen = Llio.list_from Llio.float_from buf in
    let read = Llio.list_from Llio.float_from buf in
    let write = Llio.list_from Llio.float_from buf in
    let errors =
      Llio.list_from
        (Llio.pair_from
           Llio.float_from
           Llio.string_from)
        buf in
    { kind; node_id; decommissioned; other;
      total; used;
      seen; read; write; errors;
      checksum_errors = 0L;
      claimed_since = None;
    }

  let _from_buffer2 orig_buf =
    let bufs = Llio.string_from orig_buf in
    (* TODO this could be optimized to avoid a copy *)
    let buf = Llio.make_buffer bufs 0 in

    let kind_v = Llio.int8_from buf in
    let ips = Llio.list_from Llio.string_from buf in
    let port = Llio.int_from buf in
    let use_tls = Llio.bool_from buf in
    let long_id = Llio.string_from buf in
    let conn_info = ips,port,use_tls, false in
    let kind = match kind_v with
      | 1 -> Asd (conn_info, long_id)
      | 2 -> Kinetic(conn_info, long_id)
      | 3 ->
        let cfg = Alba_arakoon.Config.from_buffer buf in
        let id = Llio.string_from buf in
        let prefix = Llio.string_from buf in
        let preset = Llio.string_from buf in
        Alba { cfg; id; prefix; preset; }
      | 4 ->
        let cfg = Alba_arakoon.Config.from_buffer buf in
        let id = Llio.string_from buf in
        let prefix = Llio.string_from buf in
        let preset = Llio.string_from buf in
        Alba2 { cfg; id; prefix; preset; }
      | k -> raise_bad_tag "OsdInfo" k
    in
    let node_id = Llio.string_from buf in
    let decommissioned = Llio.bool_from buf in
    let other  = Llio.string_from buf in
    let total  = Llio.int64_from buf in
    let used   = Llio.int64_from buf in
    let seen = Llio.list_from Llio.float_from buf in
    let read = Llio.list_from Llio.float_from buf in
    let write = Llio.list_from Llio.float_from buf in
    let errors =
      Llio.list_from
        (Llio.pair_from
           Llio.float_from
           Llio.string_from)
        buf in
    { kind; node_id; decommissioned; other;
      total; used;
      seen; read; write; errors;
      checksum_errors = 0L;
      claimed_since = None;
    }

  let _from_buffer3 orig_buf =
    let bufs = Llio.string_from orig_buf in
    (* TODO this could be optimized to avoid a copy *)
    let buf = Llio.make_buffer bufs 0 in

    let kind_v = Llio.int8_from buf in
    let conn_info_and_id_from () =
      let ips = Llio.list_from Llio.string_from buf in
      let port = Llio.int_from buf in
      let use_tls  = Llio.bool_from buf in
      let use_rdma = Llio.bool_from buf in
      let conn_info = ips,port,use_tls, use_rdma in
      let long_id  = Llio.string_from buf in
      conn_info, long_id
    in
    let kind = match kind_v with
      | 1 ->
         let conn_info, long_id = conn_info_and_id_from () in
         Asd (conn_info, long_id)
      | 2 ->
         let conn_info, long_id = conn_info_and_id_from () in
         Kinetic (conn_info, long_id)
      | 3 ->
        let cfg = Alba_arakoon.Config.from_buffer buf in
        let id = Llio.string_from buf in
        let prefix = Llio.string_from buf in
        let preset = Llio.string_from buf in
        Alba { cfg; id; prefix; preset; }
      | 4 ->
        let cfg = Alba_arakoon.Config.from_buffer buf in
        let id = Llio.string_from buf in
        let prefix = Llio.string_from buf in
        let preset = Llio.string_from buf in
        Alba2 { cfg; id; prefix; preset; }
      | 5 ->
         let endpoints = Llio.list_from Llio.string_from buf in
         let id = Llio.string_from buf in
         let prefix = Llio.string_from buf in
         let preset = Llio.string_from buf in
         AlbaProxy { endpoints; id; prefix; preset; }
      | k -> raise_bad_tag "OsdInfo" k
    in
    let node_id = Llio.string_from buf in
    let decommissioned = Llio.bool_from buf in
    let other  = Llio.string_from buf in
    let total  = Llio.int64_from buf in
    let used   = Llio.int64_from buf in
    let seen = Llio.list_from Llio.float_from buf in
    let read = Llio.list_from Llio.float_from buf in
    let write = Llio.list_from Llio.float_from buf in
    let errors =
      Llio.list_from
        (Llio.pair_from
           Llio.float_from
           Llio.string_from)
        buf in
    let checksum_errors = maybe_from_buffer Llio.int64_from 0L buf in
    let claimed_since = maybe_from_buffer
                          (Llio.option_from Llio.float_from) None buf in
    { kind; node_id; decommissioned; other;
      total; used;
      seen; read; write; errors;
      checksum_errors;
      claimed_since;
    }


  let from_buffer buf =
    let ser_version = Llio.int8_from buf in
    match ser_version with
    | 3 -> _from_buffer3 buf
    | 2 -> _from_buffer2 buf
    | 1 -> _from_buffer1 buf
    | k -> raise_bad_tag "OsdInfo.ser_version" k



end

type osd_id = Preset.osd_id [@@deriving show, yojson]

module GcEpochs = struct
  type gc_epoch = Int64.t [@@deriving show]

  type t = { minimum_epoch : gc_epoch;
             next_epoch : gc_epoch; }
  [@@deriving show]

  let output buf { minimum_epoch; next_epoch } =
    Llio.int64_to buf minimum_epoch;
    Llio.int64_to buf next_epoch

  let input buf =
    let minimum_epoch = Llio.int64_from buf in
    let next_epoch = Llio.int64_from buf in
    { minimum_epoch; next_epoch }

  let initial = { minimum_epoch = 0L; next_epoch = 1L }
  let is_valid_epoch { minimum_epoch; next_epoch } gc_epoch =
    Int64.(gc_epoch >=: minimum_epoch && gc_epoch <: next_epoch)

  let get_latest_valid t =
    let latest = Int64.pred t.next_epoch in
    if is_valid_epoch t latest
    then Some latest
    else None
end

include Alba_compression (* arakoon client has a "compression" module *)

module Encoding_scheme = Preset.Encoding_scheme

type chunk_size = int
[@@deriving show]

module EncryptInfo = struct
  open Encryption

  type key_identification =
    | KeySha256 of string
  [@@deriving show, yojson]

  let id_to_buffer buf = function
    | KeySha256 id ->
      Llio.int8_to buf 1;
      Llio.string_to buf id

  let id_from_buffer buf =
    let k = Llio.int8_from buf in
    if k <> 1
    then raise_bad_tag "EncryptInfo.key_identification" k;
    KeySha256 (Llio.string_from buf)

  type t =
    | NoEncryption
    | Encrypted of Encryption.algo * key_identification
  [@@deriving show, yojson]

  let to_buffer buf = function
    | NoEncryption ->
      Llio.int8_to buf 1
    | Encrypted (algo, key_id) ->
      Llio.int8_to buf 2;
      Encryption.algo_to_buffer buf algo;
      id_to_buffer buf key_id

  let from_buffer buf =
    match Llio.int8_from buf with
    | 1 ->
      NoEncryption
    | 2 ->
      let algo = Encryption.algo_from_buffer buf in
      let id = id_from_buffer buf in
      Encrypted (algo, id)
    | k ->
      raise_bad_tag "Nsm_model.Encryption" k
end

module Storage_scheme = struct
  type t =
    | EncodeCompressEncrypt of Encoding_scheme.t * Compression.t
    (* alternative scheme which doesn't require repair to know the encryption key
       | CompressEncryptEncode of compression * encoding_scheme *)
  [@@deriving show, yojson]

  let output buf = function
    | EncodeCompressEncrypt (es, compression) ->
      Llio.int8_to buf 1;
      Encoding_scheme.output buf es;
      Compression.output buf compression

  let input buf =
    match Llio.int8_from buf with
    | 1 ->
      let es = Encoding_scheme.input buf in
      let compression = Compression.input buf in
      EncodeCompressEncrypt (es, compression)
    | k -> raise_bad_tag "Storage_scheme" k
end



include Layout

include Checksum

module DeviceSet = Set.Make(struct type t = osd_id let compare = compare end)

include Fragment

module Manifest = struct

  type t = {
    name : string;
    object_id : string;

    storage_scheme : Storage_scheme.t;
    encrypt_info : EncryptInfo.t;

    chunk_sizes : int list;
    size : Int64.t; (* size of the object *)
    checksum : Checksum.t;
    fragments : Fragment.t Layout.t;
    version_id : version;
    max_disks_per_node : int;

    timestamp : float;
  }
  [@@deriving show, yojson]

  let make ~name ~object_id
           ~storage_scheme ~encrypt_info
           ~chunk_sizes ~size
           ~checksum
           ~fragments
           ~version_id
           ~max_disks_per_node
           ~timestamp
    =
    { name; object_id;
      storage_scheme; encrypt_info;
      chunk_sizes; checksum; size;
      fragments;
      version_id;
      max_disks_per_node;
      timestamp;
    }




  let get_summed_fragment_sizes t =
    Layout.fold
      (fun acc f -> Int64.(add acc (of_int (Fragment.len_of f))))
      0L
      t.fragments

  let inner_to_buffer_1 buf t =
    Lwt_log.ign_debug_f "Manifest.inner_to_buffer_1";
    Llio.string_to buf t.name;
    Llio.string_to buf t.object_id;
    Llio.list_to Llio.int_to buf t.chunk_sizes;
    Storage_scheme.output buf t.storage_scheme;
    EncryptInfo.to_buffer buf t.encrypt_info;
    Checksum.output buf t.checksum;
    Llio.int64_to buf t.size;
    Layout.output
      (fun buf f ->
        (Llio.pair_to
           (Llio.option_to x_int64_to)
           Llio.int_to)
         buf (Fragment.loc_of f)
      )
      buf
      t.fragments;
    Layout.output
      (fun buf  f -> Checksum.output buf (Fragment.crc_of f))
      buf
      t.fragments;

    Layout.output
      (fun buf f ->
        Llio.int_to buf (Fragment.len_of f))
      buf
      t.fragments;

    Llio.int_to buf t.version_id;
    Llio.int_to buf t.max_disks_per_node;
    Llio.float_to buf t.timestamp;
    Layout.output
      (fun buf f ->
        (Llio.option_to Llio.string_to)
          buf
          (Fragment.ctr_of f)
      )
      buf
      t.fragments;
    ()

  let inner_to_buffer_2 buf t =
    Lwt_log.ign_debug_f "Manifest.inner_to_buffer_2";
    Llio.string_to buf t.name;
    Llio.string_to buf t.object_id;
    Llio.list_to Llio.int_to buf t.chunk_sizes;
    Storage_scheme.output buf t.storage_scheme;
    EncryptInfo.to_buffer buf t.encrypt_info;
    Checksum.output buf t.checksum;
    Llio.int64_to buf t.size;
    Layout.output Fragment.fragment_to buf t.fragments;
    Llio.int_to buf t.version_id;
    Llio.int_to buf t.max_disks_per_node;
    Llio.float_to buf t.timestamp


  let to_buffer ~version buf t =
    Llio.int8_to buf version;
    let ser = match version with
      | 1 -> inner_to_buffer_1
      | 2 -> inner_to_buffer_2
      | _ -> failwith "unsupported version"
    in
    let res = serialize ser t in
    Llio.string_to buf (Snappy.compress res)

  let inner_from_buffer_1 buf =
    let name = Llio.string_from buf in
    let object_id = Llio.string_from buf in
    let chunk_sizes = Llio.list_from Llio.int_from buf in
    let storage_scheme = Storage_scheme.input buf in
    let encrypt_info = EncryptInfo.from_buffer buf in
    let checksum = Checksum.input buf in
    let size = Llio.int64_from buf in
    let fragment_locations =
      Layout.input
        (Llio.pair_from
           (Llio.option_from x_int64_from)
           Llio.int_from)
        buf in
    let fragment_checksums = Layout.input Checksum.input buf in
    let fragment_packed_sizes = Layout.input Llio.int_from buf in
    let version_id = Llio.int_from buf in
    let max_disks_per_node = Llio.int_from buf in
    let timestamp = Llio.float_from buf in
    let fragment_ctrs = maybe_from_buffer
                          (Layout.input (Llio.option_from Llio.string_from))
                          (Layout.map (fun _ -> None) fragment_checksums)
                          buf
    in
    let fragments = Layout.map4
                      Fragment.make'
                      fragment_locations
                      fragment_checksums
                      fragment_packed_sizes
                      fragment_ctrs
    in
    make ~name ~object_id
         ~storage_scheme ~encrypt_info
         ~chunk_sizes ~checksum ~size
         ~fragments
         ~version_id
         ~max_disks_per_node
         ~timestamp

  let inner_from_buffer_2 buf : t =
    Lwt_log.ign_debug_f "Manifest.inner_from_buffer_2";
    let name = Llio.string_from buf in
    let object_id = Llio.string_from buf in
    let chunk_sizes = Llio.list_from Llio.int_from buf in
    let storage_scheme = Storage_scheme.input buf in
    let encrypt_info = EncryptInfo.from_buffer buf in
    let checksum = Checksum.input buf in
    let size = Llio.int64_from buf in
    let fragments = Layout.input Fragment.fragment_from buf in
    let version_id = Llio.int_from buf in
    let max_disks_per_node = Llio.int_from buf in
    let timestamp = Llio.float_from buf in
    make ~name ~object_id
         ~chunk_sizes
         ~storage_scheme ~encrypt_info
         ~size ~checksum
         ~fragments
         ~version_id
         ~max_disks_per_node
         ~timestamp

  let from_buffer buf =
    let inflater =
      match Llio.int8_from buf with
      | 1 -> inner_from_buffer_1
      | 2 -> inner_from_buffer_2
      | k -> raise_bad_tag "Nsm_model.Manifest" k
    in
    let s = Snappy.uncompress (Llio.string_from buf) in
    deserialize inflater s

  let input = from_buffer

  let osds_used' ~loc_of fragment_locations : DeviceSet.t =
    Layout.fold
      (fun ds x ->
        match fst (loc_of x) with
        | None -> ds
        | Some osd_id -> DeviceSet.add osd_id ds
        )
      DeviceSet.empty
      fragment_locations

  let osds_used t = osds_used' ~loc_of:Fragment.loc_of t.fragments

  let locations t = Layout.map Fragment.loc_of t.fragments
  let counters t = Layout.map Fragment.ctr_of t.fragments

  let get_packed_size t chunk_id fragment_id =
    Layout.index t.fragments chunk_id fragment_id |> Fragment.len_of

  let get_location t chunk_id fragment_id =
    Layout.index t.fragments chunk_id fragment_id |> Fragment.loc_of

  let get_checksum t chunk_id fragment_id =
    Layout.index t.fragments chunk_id fragment_id |> Fragment.crc_of

  let n_chunks t = List.length t.chunk_sizes

  let chunk_size t chunk_id = List.nth_exn t.chunk_sizes chunk_id

  let combined_fragment_infos mf =
    Layout.map
      (fun f -> let open Fragment in
                (loc_of f, crc_of f, ctr_of f)) mf.fragments

  let has_holes t =
    Layout.fold
      (fun acc f  ->
        let osd_id_o = Fragment.osd_of f in
        acc || osd_id_o = None)
      false
      t.fragments

  let has_data_fragments k t =
    let data_fragments =
      Layout.map_indexed
        (fun chunk_index fragment_index fragment ->
          fragment_index < k && Fragment.has_osd fragment
        )
      t.fragments
    in
    Layout.fold (fun acc f -> acc || f) false data_fragments
end

include Fragment_update

module Assert =
  struct
    type t =
      | ObjectExists of object_name
      | ObjectDoesNotExist of object_name
      | ObjectHasId of object_name * object_id
      | ObjectHasChecksum of object_name * Checksum.t

    let to_buffer buf = function
      | ObjectExists name ->
         Llio.int8_to buf 1;
         Llio.string_to buf name
      | ObjectDoesNotExist name ->
         Llio.int8_to buf 2;
         Llio.string_to buf name
      | ObjectHasId (name, id) ->
         Llio.int8_to buf 3;
         Llio.string_to buf name;
         Llio.string_to buf id
      | ObjectHasChecksum (name, cs) ->
         Llio.int8_to buf 4;
         Llio.string_to buf name;
         Checksum.to_buffer buf cs

    let from_buffer buf =
      match Llio.int8_from buf with
      | 1 ->
         let name = Llio.string_from buf in
         ObjectExists name
      | 2 ->
         let name = Llio.string_from buf in
         ObjectDoesNotExist name
      | 3 ->
         let name = Llio.string_from buf in
         let id = Llio.string_from buf in
         ObjectHasId (name, id)
      | 4 ->
         let name = Llio.string_from buf in
         let cs = Checksum.from_buffer buf in
         ObjectHasChecksum (name, cs)
     | k -> raise_bad_tag "Nsm_model.Assert" k
  end

module Update =
  struct
    type t =
      | PutObject of Manifest.t * GcEpochs.gc_epoch
      | DeleteObject of object_name

    let to_buffer ~manifest_version buf = function
      | PutObject (mf, gc_epoch) ->
         Llio.int8_to buf 1;
         Manifest.to_buffer ~version:manifest_version buf mf;
         Llio.int64_to buf gc_epoch
      | DeleteObject name ->
         Llio.int8_to buf 2;
         Llio.string_to buf name

    let from_buffer buf =
      match Llio.int8_from buf with
      | 1 ->
         let mf = Manifest.from_buffer buf in
         let gc_epoch = Llio.int64_from buf in
         PutObject (mf, gc_epoch)
      | 2 ->
         let name = Llio.string_from buf in
         DeleteObject name
      | k -> raise_bad_tag "Nsm_model.Update" k
  end

module ObjectInfo = struct
  type t =
    | ManifestWithObjectId of object_id

  let get_object_id = function
    | ManifestWithObjectId object_id -> object_id

  let to_buffer buf = function
    | ManifestWithObjectId object_id ->
      Llio.int8_to buf 1;
      Llio.string_to buf object_id

  let from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> ManifestWithObjectId (Llio.string_from buf)
    | k -> raise_bad_tag "Nsm_model.ObjectInfo" k
end

type overwrite =
  | Unconditionally
  | NoPrevious
  | PreviousObjectId of object_id
  | AnyPrevious

module NamespaceStats = struct
  type t = {
    logical_size : int64;
    storage_size : int64;
    storage_size_per_osd : (osd_id * int64) Std.counted_list;
    bucket_count : (Policy.policy * int64) Std.counted_list;
  }

  let to_buffer buf { logical_size;
                      storage_size;
                      storage_size_per_osd;
                      bucket_count; } =
    let ser_version = 1 in Llio.int8_to buf ser_version;
    Llio.int64_to buf logical_size;
    Llio.int64_to buf storage_size;
    Llio.counted_list_to
      (Llio.pair_to
         x_int64_to
         Llio.int64_to)
      buf
      storage_size_per_osd;
    Llio.counted_list_to
      (Llio.pair_to
         Policy.to_buffer
         Llio.int64_to)
      buf
      bucket_count

  let from_buffer buf =
    let ser_version = Llio.int8_from buf in
    assert (ser_version = 1);
    let logical_size = Llio.int64_from buf in
    let storage_size = Llio.int64_from buf in
    let storage_size_per_osd =
      Llio.counted_list_from
        (Llio.pair_from
           x_int64_from
           Llio.int64_from)
        buf in
    let bucket_count =
      Llio.counted_list_from
        (Llio.pair_from
           Policy.from_buffer
           Llio.int64_from)
        buf in
    { logical_size;
      storage_size;
      storage_size_per_osd;
      bucket_count; }

end

module Keys = struct
  (* map an object_name to an object_id *)
  let names name = Printf.sprintf "names/%s" name
  let names_next_prefix = next_prefix "names/"
  let names_extract_name =
    let prefix_len = String.length (names "") in
    fun key -> Str.string_after key prefix_len

  (* map an object_id (which is unique) to a manifest *)
  let objects_prefix = "objects/"
  let objects ~object_id = objects_prefix ^ object_id
  let objects_next_prefix = next_prefix objects_prefix
  let objects_extract_id =
    let prefix_len = String.length objects_prefix in
    fun key -> Str.string_after key prefix_len

  (* this contains the range of gc epochs for which
     fragments can be added to this namespace
     the range is expressed as (minimum_epoch, next_epoch).
     It is possible that there are currently no valid gc epochs.
  *)
  let gc_epochs =
    Printf.sprintf "gc_epochs"

  let namespace_logical_size = "namespace_logical_size"
  let namespace_storage_size = "namespace_storage_size"

  let policies_prefix = "policies"
  let policies_next_prefix = next_prefix policies_prefix
  let policies ~k ~m ~fragment_count ~max_disks_per_node ~object_id =
    Printf.sprintf
      "%s/%s"
      policies_prefix
      (serialize
         (Llio.tuple5_to
            Llio.int32_be_to
            Llio.int32_be_to
            Llio.int32_be_to
            Llio.int32_be_to
            Llio.string_to)
         (Int32.of_int k,
          Int32.of_int m,
          Int32.of_int fragment_count,
          Int32.of_int (-max_disks_per_node),
          object_id))
  let parse_policies_key =
    let prefix_len = String.length policies_prefix + 1 in
    fun key ->
      deserialize
        (Llio.tuple5_from
           Llio.int32_be_from
           Llio.int32_be_from
           Llio.int32_be_from
           (fun buf -> Int32.neg (Llio.int32_be_from buf))
           Llio.string_from)
        (Str.string_after key prefix_len)

  let policies_cnt_prefix = "policies_cnt"
  let policies_cnt_next_prefix = next_prefix policies_cnt_prefix
  let policies_cnt ~k ~m ~fragment_count ~max_disks_per_node =
    Printf.sprintf
      "%s/%s"
      policies_cnt_prefix
      (serialize
        (Llio.tuple4_to
           Llio.int32_be_to
           Llio.int32_be_to
           Llio.int32_be_to
           Llio.int32_be_to)
        (Int32.of_int k,
         Int32.of_int m,
         Int32.of_int fragment_count,
         Int32.of_int max_disks_per_node))
  let parse_policies_cnt =
    let prefix_len = String.length policies_cnt_prefix + 1 in
    fun key ->
      deserialize
        (Llio.tuple4_from
           Llio.int32_be_from
           Llio.int32_be_from
           Llio.int32_be_from
           Llio.int32_be_from)
        (Str.string_after key prefix_len)

  let preset = "preset"
  let preset_version = "preset_version"

  module Device = struct

    let s device_id = serialize x_int64_be_to device_id

    let extract_osd_id_from ~prefix_len =
      deserialize
        ~offset:prefix_len
        x_int64_be_from

    let info ~osd_id = "osds/info/" ^ (s osd_id)

    let active_osds_prefix = "osds/active/"
    let active_osds_next_prefix = next_prefix active_osds_prefix
    let active_osds ~osd_id = active_osds_prefix ^ (s osd_id)
    let active_osds_extract_osd_id =
      let prefix_len = String.length active_osds_prefix in
      extract_osd_id_from ~prefix_len

    (* listing of all objects that have fragments on this device, to be used during rebuild/repair *)
    let objects_prefix_prefix = "osds/objects/"
    let objects_prefix device_id =
      Printf.sprintf "%s%s/" objects_prefix_prefix (s device_id)
    let objects device_id object_id =
      (objects_prefix device_id) ^ object_id
    let objects_next_prefix device_id =
      next_prefix (objects_prefix device_id)
    let objects_extract_object_id key =
      let osd_id =
        extract_osd_id_from
          ~prefix_len:(String.length objects_prefix_prefix)
          key
      in
      let prefix_len = String.length (objects_prefix osd_id) in
      Str.string_after key prefix_len

    (* set of keys still to be deleted from a device *)
    let keys_to_be_deleted device_id =
      Printf.sprintf "osds/deletes/%s/" (s device_id)
    let keys_to_be_deleted_next_prefix device_id =
      next_prefix (Printf.sprintf "osds/deletes/%s" (s device_id))
    let keys_to_be_deleted_extract_key key =
      let osd_id =
        extract_osd_id_from
          ~prefix_len:(String.length objects_prefix_prefix)
          key
      in
      let prefix_len = String.length (objects_prefix osd_id) in
      Str.string_after key prefix_len

    let size_prefix = "osds/size/"
    let size osd_id = Printf.sprintf "%s%s" size_prefix (s osd_id)
    let size_next_prefix = next_prefix size_prefix
    let size_extract_osd_id =
      let prefix_len = String.length size_prefix in
      extract_osd_id_from ~prefix_len
  end
end

module type Constants = sig
  val namespace_id : int64
end

module Err = struct
  type t =
    | Unknown                 [@value 1]
    | Invalid_gc_epoch        [@value 5]
    | Object_not_found        [@value 6]
    | Non_unique_object_id    [@value 7]
    | Namespace_id_not_found  [@value 8]
    | Overwrite_not_allowed   [@value 9]
    | Not_master              [@value 10]
    | InvalidVersionId        [@value 11]
    | Old_plugin_version      [@value 12]
    | Unknown_operation       [@value 13]
    | Inconsistent_read       [@value 14]
    | Old_timestamp           [@value 15]
    | Invalid_fragment_spread [@value 16]
    | Inactive_osd            [@value 17]
    | Too_many_disks_per_node [@value 18]
    | Insufficient_fragments  [@value 19]
    | Assert_failed           [@value 20]
    | Invalid_bucket          [@value 21]
    | Preset_violated         [@value 22]
  [@@deriving show, enum]

  exception Nsm_exn of t * string

  let failwith ?(payload="") err = raise (Nsm_exn (err, payload))

  let err2int = to_enum
  let int2err x = Option.get_some_default Unknown (of_enum x)
end


let check_fragment_osd_spread manifest =
  List.iter
    (fun chunk ->
      (* for each chunk check if all fragments are stored on different osds *)
      let (_ : DeviceSet.t) =
        List.fold_left
          (fun acc f ->
            match Fragment.osd_of f with
            | None -> acc
            | Some osd_id  ->
               if DeviceSet.mem osd_id acc
               then
                 let locations = List.map Fragment.loc_of chunk in
                 Err.(failwith
                        ~payload:([%show : (int64 option * int) list] locations)
                        Invalid_fragment_spread)
               else DeviceSet.add osd_id acc)
          DeviceSet.empty
          chunk
      in
      ())
    manifest.Manifest.fragments

module Update' = Key_value_store.Update

module Preset_cache =
  struct
    let t = Hashtbl.create 3
    let get ~namespace_id ~version =
      match Hashtbl.find t namespace_id with
      | exception Not_found -> None
      | (version', preset) ->
         if version' = version
         then Some preset
         else None

    let store ~namespace_id ~version ~preset =
      Hashtbl.replace t namespace_id (version, preset)
  end

module NamespaceManager(C : Constants)(KV : Read_key_value_store) = struct

  module EKV = Read_store_extensions(KV)

  let link_osd kv osd_id osd_info =
    let blob = serialize (OsdInfo.to_buffer ~version:3) osd_info in
    [
      Update'.set (Keys.Device.active_osds ~osd_id) "";
      Update'.set (Keys.Device.info ~osd_id)        blob;
    ]

  let unlink_osd kv osd_id =
    [ Update'.delete (Keys.Device.active_osds ~osd_id); ]

  let get_osd_info kv osd_id =
    let key = Keys.Device.info ~osd_id in
    let blob = KV.get_exn kv key in
    deserialize OsdInfo.from_buffer blob

  let list_active_osds kv ~first ~finc ~last ~max ~reverse =
    EKV.map_range
      kv
      ~first:(Keys.Device.active_osds ~osd_id:first) ~finc
      ~last:(match last with
          | Some (last, linc) ->
            Some (Keys.Device.active_osds ~osd_id:last, linc)
          | None -> Keys.Device.active_osds_next_prefix)
      ~max:(cap_max ~max ()) ~reverse
      (fun cur key -> Keys.Device.active_osds_extract_osd_id key)

  let get_object_manifest_by_id kv object_id =
    match KV.get kv (Keys.objects ~object_id) with
    | None -> Err.failwith Err.Object_not_found
    | Some manifest_s -> deserialize Manifest.input manifest_s, manifest_s

  let get_object_manifest_by_name kv name =
    Option.map
      (fun object_info_s ->
        let object_info = deserialize ObjectInfo.from_buffer object_info_s in
        let object_id = ObjectInfo.get_object_id object_info in
        let manifest_s = KV.get_exn kv (Keys.objects ~object_id) in
        let manifest = deserialize Manifest.input manifest_s in
        manifest)
      (KV.get kv (Keys.names name))

  let get_osd_size_updates ~delete manifest =
    List.fold_left
      (fun acc fds ->
         let acc' =
           List.fold_left
             (fun a f ->
               match f.Fragment.len, f.Fragment.osd with
                | (_,             None) -> a
                | (fragment_size, Some osd_id) ->
                  let upd =
                    Update'.add
                      (Keys.Device.size osd_id)
                      (Int64.of_int (if delete
                                     then -fragment_size
                                     else fragment_size)) in
                  upd::a)
             acc
             fds
         in
         acc')
      []
      manifest.Manifest.fragments

  let get_preset kv =
    match KV.get kv Keys.preset_version with
    | None -> None
    | Some version ->
       match Preset_cache.get ~namespace_id:C.namespace_id ~version with
       | None ->
          let preset =
            KV.get_exn kv Keys.preset
            |> deserialize Preset.from_buffer
          in
          Preset_cache.store ~namespace_id:C.namespace_id ~version ~preset;
          Some preset
       | Some preset ->
          Some preset

  let get_min_fragment_count_and_max_disks_per_node
        kv ~k ~m fragments ~validate
    =
    let get_bla_per_chunk chunk_fragments =
      let osds_per_node = Hashtbl.create 3 in
      List.fold_left
        (fun (effective_fragment_count, effective_max_disks_per_node) fragment ->
          match Fragment.osd_of fragment with
          | None ->
             (effective_fragment_count,
              effective_max_disks_per_node)
          | Some osd_id ->
             let osd_info = get_osd_info kv osd_id in
             let node_id = osd_info.OsdInfo.node_id in
             let cnt =
               try Hashtbl.find osds_per_node node_id
               with Not_found -> 0
             in

             let cnt' = cnt + 1 in

             Hashtbl.replace osds_per_node node_id cnt';
             (effective_fragment_count + 1,
              max effective_max_disks_per_node cnt'))
        (0, 0)
        chunk_fragments
    in
    let min_fragment_count, max_disks_per_node =
      List.fold_left
        (fun (effective_fragment_count, effective_max_disks_per_node)
             chunk_fragments ->
          let effective_fragment_count', effective_max_disks_per_node' =
            get_bla_per_chunk chunk_fragments
          in
          (min effective_fragment_count effective_fragment_count',
           max effective_max_disks_per_node effective_max_disks_per_node'))
        (max_int, 0)
        fragments
    in

    let maybe_preset = get_preset kv in
    let () =
      match validate, maybe_preset with
      | _, None -> ()
      | false, _ -> ()
      | true, Some p ->
         (* bucket = k, m, min_fragment_count, max_disks_per_node
          * match bucket with policy ... if we can't -> throw error *)
         let policy =
           List.find
             (fun (k', m', min_fragment_count', max_disks_per_node') ->
               k = k' && m = m'
               && min_fragment_count >= min_fragment_count'
               && max_disks_per_node <= max_disks_per_node')
             p.Preset.policies
         in
         match policy with
         | None -> Err.(failwith
                          ~payload:(Printf.sprintf
                                      "bucket %s not valid for policies %s"
                                      ([%show : int * int * int * int] (k, m, min_fragment_count, max_disks_per_node))
                                      ([%show : (int * int * int * int) list] p.Preset.policies)
                                   )
                          Invalid_bucket)
         | Some _ -> ()
    in

    (min_fragment_count, max_disks_per_node)

  (* for backwards compatibility we store the keys to
   * be deleted as global keys
   *)
  let to_global_key key =
    Osd_keys.AlbaInstance.to_global_key
      C.namespace_id
      (key, 0, String.length key)

  let cleanup_for_object_id kv old_object_id =
    let object_key = Keys.objects ~object_id:old_object_id in
    let old_manifest, old_manifest_s = get_object_manifest_by_id kv old_object_id in
    let delete_manifest =
      Update'.compare_and_swap
        object_key
        (Some old_manifest_s)
        None in
    let open Manifest in
    let from_chunk_fragments chunk_id chunk_fragments =
      List.fold_left
        (fun (fragment_id, deletes) fragment ->
           let deletes' = match Fragment.loc_of fragment with
             | None, _ -> []
             | Some device_id, version_id ->
               let keys_to_be_deleted =
                 [ Osd_keys.AlbaInstance.fragment
                     ~object_id:old_object_id
                     ~version_id
                     ~chunk_id
                     ~fragment_id
                   |> to_global_key;
                   Osd_keys.AlbaInstance.fragment_recovery_info
                     ~object_id:old_object_id
                     ~version_id
                     ~chunk_id
                     ~fragment_id
                   |> to_global_key;
                 ] in
               List.map
                 (fun key -> Update'.set ((Keys.Device.keys_to_be_deleted device_id) ^ key) "")
                 keys_to_be_deleted
           in
           (fragment_id + 1, List.rev_append deletes' deletes))
        (0, [])
        chunk_fragments in
    let _, delete_fragments_and_recovery_info =
      List.fold_left
        (fun (chunk_id, acc_delete_fragments) chunk_fragments ->
           let _, delete_fragments = from_chunk_fragments chunk_id chunk_fragments in
           chunk_id + 1,
           List.rev_append delete_fragments acc_delete_fragments)
        (0, [])
        old_manifest.fragments
    in
    let update_osd_sizes = get_osd_size_updates ~delete:true old_manifest in

    let delete_from_device_objects =
      let old_devices = Manifest.osds_used old_manifest in
      DeviceSet.fold
        (fun device_id acc ->
          Update'.delete (Keys.Device.objects device_id old_object_id) :: acc)
        old_devices
        [] in
    let delete_policy =
      let Storage_scheme.EncodeCompressEncrypt
          (Encoding_scheme.RSVM (k, m, _), _) =
        old_manifest.storage_scheme in
      let fragment_count, max_disks_per_node =
        get_min_fragment_count_and_max_disks_per_node
          kv
          ~k ~m
          old_manifest.fragments
          ~validate:false
      in
      [ Update'.delete (Keys.policies ~k ~m ~fragment_count ~max_disks_per_node ~object_id:old_object_id);
        Update'.add (Keys.policies_cnt ~k ~m ~fragment_count ~max_disks_per_node) (-1L); ]
    in
    (List.concat [delete_manifest;
                  delete_policy;
                  delete_fragments_and_recovery_info;
                  delete_from_device_objects;
                  update_osd_sizes;
                 ],
     old_manifest)

  let get_gc_epochs kv =
    let gc_epoch_so = KV.get kv Keys.gc_epochs in
    let gc_epochs =
      match gc_epoch_so with
      | None -> GcEpochs.initial
      | Some gc_epoch_s -> deserialize GcEpochs.input gc_epoch_s in
    (gc_epoch_so, gc_epochs)

  let disable_gc_epoch kv gc_epoch =
    let open GcEpochs in
    let gc_epoch_so, { minimum_epoch; next_epoch } =
      get_gc_epochs kv in
     if Int64.(minimum_epoch =: gc_epoch) &&
        Int64.(gc_epoch <: next_epoch)
     then
       Update'.compare_and_swap
         Keys.gc_epochs
         gc_epoch_so
         (Some (serialize
                  GcEpochs.output
                  { minimum_epoch = (Int64.succ gc_epoch);
                    next_epoch = next_epoch; }))
     else
       Err.failwith Err.Invalid_gc_epoch

  let enable_new_gc_epoch kv gc_epoch =
    let open GcEpochs in
    let gc_epoch_so, { minimum_epoch; next_epoch } =
      get_gc_epochs kv in
    if Int64.(gc_epoch =: next_epoch)
    then
      Update'.compare_and_swap
        Keys.gc_epochs
        gc_epoch_so
        (Some (serialize
                 GcEpochs.output
                 { minimum_epoch;
                   next_epoch = (Int64.succ gc_epoch); }))
    else
      Err.failwith Err.Invalid_gc_epoch

  let update_device_object_mapping
      kv object_id
      ~old_locations ~new_fragments
      ~max_disks_per_node =

    (* in old manifest there is a set of devices, in the updated manifest there
       is a different set, so the mapping containing the objects on each disk
       has to be updated
       S1 \ S2 -> some keys to be removed
       S2 \ S1 -> some keys to be added *)

    let old_devices = Manifest.osds_used' ~loc_of:(fun x -> x) old_locations in
    let new_devices = Manifest.osds_used' ~loc_of:Fragment.loc_of new_fragments in

    let remove_upds =
      let removed_from_devices = DeviceSet.diff old_devices new_devices in
      DeviceSet.fold
        (fun device_id acc ->
           Update'.delete (Keys.Device.objects device_id object_id) :: acc)
        removed_from_devices
        [] in
    let add_upds =
      let added_to_devices = DeviceSet.diff new_devices old_devices in
      DeviceSet.fold
        (fun osd_id acc ->

           let active_osd_key = Keys.Device.active_osds ~osd_id in
           let osd_info_so = KV.get kv active_osd_key in
           if osd_info_so = None
           then Err.(failwith ~payload:(Int64.to_string osd_id) Inactive_osd);

           Update'.Assert (active_osd_key, osd_info_so) ::
           Update'.set (Keys.Device.objects osd_id object_id) "" ::
           acc)
        added_to_devices
        [] in
    List.rev_append
      add_upds
      remove_upds

  let check_overwrite object_id_o = function
    | Unconditionally -> ()
    | NoPrevious ->
      if object_id_o <> None
      then Err.failwith Err.Overwrite_not_allowed
    | PreviousObjectId object_id ->
      if object_id_o <> Some object_id
      then Err.failwith Err.Overwrite_not_allowed
    | AnyPrevious ->
      if object_id_o = None
      then Err.failwith Err.Overwrite_not_allowed

  let put_object kv overwrite manifest gc_epoch =
    let maybe_preset = get_preset kv in
    let () =
      match maybe_preset with
      | None -> ()
      | Some p ->
         (* verify object upload is according to preset *)

         let Storage_scheme.EncodeCompressEncrypt
               (Encoding_scheme.RSVM (k, m, w), compression) =
           manifest.Manifest.storage_scheme in

         List.iter
           (fun chunk_size ->
             if chunk_size / k > p.Preset.fragment_size
             then Err.(failwith Preset_violated ~payload:"fragment_size mismatch"))
           manifest.Manifest.chunk_sizes;

         let () =
           let open EncryptInfo in
           let open Encryption in
           match manifest.Manifest.encrypt_info, p.Preset.fragment_encryption with
           | NoEncryption, Encryption.NoEncryption -> ()
           | NoEncryption, _ -> Err.(failwith Preset_violated
                                              ~payload:"preset requires encryption")
           | Encrypted (algo1, key_identification), Encryption.AlgoWithKey (algo2, key) ->
              if algo1 <> algo2 (* || key_identification <> Encrypt_info_helper.get_id_for_key key *)
              then Err.(failwith Preset_violated
                                 ~payload:"preset needs different encryption")
           | Encrypted _, _ -> Err.(failwith Preset_violated ~payload:"encryption")
         in

         List.iter
           (List.iter
              (fun f ->
                let cs = Fragment.crc_of f in
                if p.Preset.fragment_checksum_algo <> Checksum.algo_of cs
                then Err.(failwith Preset_violated
                                   ~payload:"fragment_checksum mismatch")))
           manifest.Manifest.fragments;

         if not (List.mem
                   (Checksum.algo_of manifest.Manifest.checksum)
                   Preset.(p.object_checksum.allowed))
         then Err.(failwith Preset_violated ~payload:"object_checksum mismatch");

         if w <> p.Preset.w
         then Err.(failwith Preset_violated ~payload:"w mismatch");

         if compression <> p.Preset.compression
         then Err.(failwith Preset_violated ~payload:"compression mismatch");

         ()
    in

    let object_name = manifest.Manifest.name in
    let object_id = manifest.Manifest.object_id in

    (* sanity check to see if the client is really generating unique object_ids *)
    if KV.exists kv (Keys.objects ~object_id)
    then Err.failwith Err.Non_unique_object_id;

    check_fragment_osd_spread manifest;

    let open Manifest in
    let name_key = Keys.names object_name in
    let old_object_info_o = KV.get kv name_key in
    let old_manifest_o =
      Option.map (fun s -> deserialize ObjectInfo.from_buffer s)
                 old_object_info_o
    in
    let old_object_id_o =
      Option.map ObjectInfo.get_object_id old_manifest_o
    in
    check_overwrite old_object_id_o overwrite;
    let update_name =
      Update'.compare_and_swap
        name_key
        old_object_info_o
        (Some (serialize
                 ObjectInfo.to_buffer
                 (ObjectInfo.ManifestWithObjectId object_id))) in
    let add_manifest =
      Update'.compare_and_swap
        (Keys.objects ~object_id)
        None
        (Some (serialize (Manifest.to_buffer
                            ~version:1 (* TODO: should become 2 *)) manifest))
    in

    let gc_epoch_so, gc_epochs = get_gc_epochs kv in
    let assert_gc_epoch =
      if not (GcEpochs.is_valid_epoch gc_epochs gc_epoch)
      then Err.failwith Err.Invalid_gc_epoch
      else Update'.Assert (Keys.gc_epochs, gc_epoch_so)
    in

    let update_osd_sizes = get_osd_size_updates ~delete:false manifest in

    let clean_old_info,
        old_info_o,
        logical_delta,
        storage_delta
      =
      match old_object_id_o with
       | None -> [], None, manifest.size, Manifest.get_summed_fragment_sizes manifest
       | Some old_object_id ->
         let upds, old_manifest = cleanup_for_object_id kv old_object_id in

         let old_timestamp = old_manifest.Manifest.timestamp in
         if manifest.Manifest.timestamp < old_timestamp
         then Err.(failwith ~payload:(serialize Llio.float_to old_timestamp) Old_timestamp);

         let logical_delta = Int64.sub manifest.size old_manifest.size in
         let storage_delta = Int64.sub
                               (Manifest.get_summed_fragment_sizes manifest)
                               (Manifest.get_summed_fragment_sizes old_manifest)
         in
         upds,
         Some old_manifest,
         logical_delta, storage_delta
    in
    let update_logical_size =
      Update'.add Keys.namespace_logical_size logical_delta in
    let update_storage_size =
      Update'.add Keys.namespace_storage_size storage_delta in

    let device_obj_mapping_upds =
      update_device_object_mapping
        kv
        object_id
        ~old_locations:[]
        ~new_fragments:manifest.fragments
        ~max_disks_per_node:manifest.Manifest.max_disks_per_node
    in
    let add_policy =
      let Storage_scheme.EncodeCompressEncrypt
          (Encoding_scheme.RSVM (k, m, _), _) =
        manifest.storage_scheme in
      let fragment_count, max_disks_per_node =
        get_min_fragment_count_and_max_disks_per_node
          kv
          ~k ~m
          manifest.fragments
          ~validate:true
      in
      [ Update'.set (Keys.policies ~k ~m ~fragment_count ~max_disks_per_node ~object_id) "";
        Update'.add (Keys.policies_cnt ~k ~m ~fragment_count ~max_disks_per_node) 1L; ]
    in
    let upds =
      List.concat [ update_name;
                    add_manifest;
                    add_policy;
                    [ assert_gc_epoch;
                      update_logical_size;
                      update_storage_size; ];
                    update_osd_sizes;
                    device_obj_mapping_upds;
                    clean_old_info; ] in
    upds, old_info_o

  let delete_object kv name overwrite =
    let name_key = Keys.names name in
    let object_info_o = KV.get kv name_key in
    let object_id_o =
      Option.map
        (fun s ->
           ObjectInfo.get_object_id
             (deserialize ObjectInfo.from_buffer s))
        object_info_o
    in
    check_overwrite object_id_o overwrite;
    match object_id_o with
    | None -> [], None
    | Some object_id ->
      let upds1, manifest = cleanup_for_object_id kv object_id in
      let logical_delta =
        let key = Keys.namespace_logical_size in
        let to_add = let open Manifest in Int64.neg manifest.size in
        Update'.add key to_add
      in
      let storage_delta =
        let key = Keys.namespace_storage_size in
        let to_add =
          Int64.neg (Manifest.get_summed_fragment_sizes manifest)
        in
        Update'.add key to_add
      in
      let upds2 =
        update_device_object_mapping
          kv
          object_id
          ~old_locations:(Manifest.locations manifest)
          ~new_fragments:[]
          ~max_disks_per_node:manifest.Manifest.max_disks_per_node
      in
      (List.concat
         [ Update'.compare_and_swap name_key object_info_o None;
           upds1;
           upds2;
           [logical_delta ];
           [storage_delta ];
         ],
       Some manifest)

  let apply_sequence kv asserts updates =
    let updates_for_asserts =
      List.flatmap
        (let open Assert in
         function
         | ObjectExists name ->
            let key = Keys.names name in
            begin
              match KV.get kv key with
              | None -> Err.(failwith ~payload:name Assert_failed)
              | (Some _) as vo -> [ Update'.Assert (key, vo); ]
            end
         | ObjectDoesNotExist name ->
            let key = Keys.names name in
            begin
              match KV.get kv key with
              | None -> [ Update'.Assert (key, None); ]
              | Some _ -> Err.(failwith ~payload:name Assert_failed)
            end
         | ObjectHasId (name, object_id) ->
            let key = Keys.names name in
            begin
              match KV.get kv key with
              | None -> Err.(failwith ~payload:name Assert_failed)
              | (Some object_info_s) as vo ->
                 let object_info = deserialize ObjectInfo.from_buffer object_info_s in
                 let object_id' = ObjectInfo.get_object_id object_info in
                 if object_id = object_id'
                 then [ Update'.Assert (key, vo); ]
                 else Err.(failwith ~payload:name Assert_failed)
            end
         | ObjectHasChecksum (name, cs) ->
            let key = Keys.names name in
            begin
              match KV.get kv key with
              | None -> Err.(failwith ~payload:name Assert_failed)
              | (Some object_info_s) as vo ->
                 let object_info = deserialize ObjectInfo.from_buffer object_info_s in
                 let object_id = ObjectInfo.get_object_id object_info in
                 let manifest, _ = get_object_manifest_by_id kv object_id in
                 if manifest.Manifest.checksum = cs
                 then [ Update'.Assert (key, vo); ]
                 else Err.(failwith ~payload:name Assert_failed)
            end
        )
        asserts
    in
    let updates_for_updates =
      (* first filter out doubles for the same key
       * only the last update for the name has effect,
       * and otherwise the further update calculation is buggy
       *)
      let updates' = Hashtbl.create 3 in
      let () =
        List.iter
          (fun update ->
           let open Update in
           let name = match update with
             | PutObject (mf, gc_epoch) -> mf.Manifest.name
             | DeleteObject name -> name
           in
           Hashtbl.replace updates' name update
          )
          updates
      in
      Hashtbl.fold
        (fun _ update acc ->
         let upds =
           let open Update in
           match update with
           | PutObject (mf, gc_epoch) ->
              put_object kv Unconditionally mf gc_epoch |> fst
           | DeleteObject name ->
              delete_object kv name Unconditionally |> fst
         in
         List.rev_append upds acc
        )
        updates'
        []
      |> List.rev
    in
    List.rev_append updates_for_asserts updates_for_updates, ()

  let list_objects kv ~first ~finc ~last ~max ~reverse =
    EKV.map_range
      kv
      ~first:(Keys.names first) ~finc
      ~last:(match last with
          | Some (last,linc) -> Some (Keys.names last, linc)
          | None -> Keys.names_next_prefix)
      ~max ~reverse
      (fun cur key -> Keys.names_extract_name key)

  let list_objects_by_id kv ~first ~finc ~last ~max ~reverse =
    EKV.map_range
      kv
      ~first:(Keys.objects ~object_id:first) ~finc
      ~last:(match last with
             | Some (last, linc) -> Some (Keys.objects ~object_id:last, linc)
             | None -> Keys.objects_next_prefix)
      ~max ~reverse
      (fun cur key -> KV.cur_get_value cur |> deserialize Manifest.input)

  let multi_exists kv object_names =
    List.map
      (fun name -> KV.exists kv (Keys.names name))
      object_names

  let list_device_objects kv device_id ~first ~finc ~last ~max ~reverse =
    EKV.map_range
      kv
      ~first:(Keys.Device.objects device_id first) ~finc
      ~last:(match last with
          | Some (last, linc) -> Some (Keys.Device.objects device_id last, linc)
          | None -> Keys.Device.objects_next_prefix device_id)
      ~max ~reverse
      (fun cur key ->
         let object_id = Keys.Device.objects_extract_object_id key in
         let manifest, _ = get_object_manifest_by_id kv object_id in
         manifest)

  let list_device_keys_to_be_deleted kv device_id ~first ~finc ~last ~max ~reverse =
    EKV.map_range
      kv
      ~first:((Keys.Device.keys_to_be_deleted device_id) ^ first) ~finc
      ~last:(match last with
          | Some (last, linc) -> Some ((Keys.Device.keys_to_be_deleted device_id) ^ last, linc)
          | None -> Keys.Device.keys_to_be_deleted_next_prefix device_id)
      ~max ~reverse
      (fun cur key -> Keys.Device.keys_to_be_deleted_extract_key key)

  let mark_keys_deleted kv device_keys =
    List.flatten_unordered
      (List.map
         (fun (device_id, keys) ->
            List.map
              (fun key ->
                 Update'.delete ((Keys.Device.keys_to_be_deleted device_id) ^ key))
              keys)
         device_keys)

  let cleanup_osd_keys_to_be_deleted kv osd_id =
    let (cnt, keys), _ =
      list_device_keys_to_be_deleted
        kv osd_id
        ~first:"" ~finc:true ~last:None
        ~max:(-1) ~reverse:false
    in
    mark_keys_deleted kv [ (osd_id, keys); ], cnt


  let update_manifest_generic
      kv object_name object_id
      (new_fragments : FragmentUpdate.t list)
      gc_epoch
      version_id
    =
    let assert_gc_epoch =
      let gc_epoch_so, min_max_epoch = get_gc_epochs kv in
      if not (GcEpochs.is_valid_epoch min_max_epoch gc_epoch)
      then Err.failwith Err.Invalid_gc_epoch
      else Update'.Assert (Keys.gc_epochs, gc_epoch_so)
    in
    let fragments = Hashtbl.create 3 in
    List.iter
      (fun fu ->
        let open FragmentUpdate in
        let key = fu.chunk_id, fu.fragment_id in
        if Hashtbl.mem fragments key
        then
          (* we don't want multiple updates for the same fragment,
           * it indicates a buggy client! *)
          Err.(failwith ~payload:"duplicate chunk_id,fragment_id for update_manifest" Unknown)
        else
          Hashtbl.add
            fragments
            key
            (fu, version_id)
      )
      new_fragments;
    let manifest_old, manifest_old_s = get_object_manifest_by_id kv object_id in
    let version_id_old = manifest_old.Manifest.version_id in
    assert (object_name = manifest_old.Manifest.name);
    let expected = version_id_old + 1 in
    if (version_id = 0 && version_id_old <> 0)
     || (version_id <> 0 && expected <> version_id)
    then
      Err.failwith
        Err.InvalidVersionId
        ~payload:(Printf.sprintf "InvalidVersionId:(expected:%i, got: %i)"
                                 expected version_id);

    let old_manifest_locations = Manifest.locations manifest_old in

    let update_osd_sizes =
      List.fold_left
        (fun acc fu ->
          let open FragmentUpdate in
          let old_fragment_size =
            Manifest.get_packed_size manifest_old fu.chunk_id fu.fragment_id
           in
           let upd1 = match fu.osd_id_o, fu.size_change with
             | None, None -> []
             | None, Some _ ->
                (* fragment has new checksum, but was stored nowhere *)
                []
             | Some osd_id , None ->
               [ Update'.add
                   (Keys.Device.size osd_id)
                   (Int64.of_int old_fragment_size) ]
             | Some osd_id, Some (new_size, _new_checksum) ->
                [ Update'.add
                    (Keys.Device.size osd_id)
                    (Int64.of_int new_size)
                ]
           in
           let upd2 =
             match
               Manifest.get_location manifest_old fu.chunk_id fu.fragment_id
             with
             | None, _ -> []
             | Some previous_osd_id, _ ->
               [ Update'.add
                   (Keys.Device.size previous_osd_id)
                   (Int64.of_int (-old_fragment_size)) ]
           in
           List.flatten_unordered [ upd1; upd2; acc ])
        []
        new_fragments
    in

    let _, locations_rev, obsolete_fragments =
      List.fold_left
        (fun (chunk_id, locs, obsolete_fragments) chunk_locations ->
           let _, locs_rev, obsolete_fragments' =
             List.fold_left
               (fun (fragment_id, locs, obsolete_fragments) old_loc ->
                  let loc_index = (chunk_id, fragment_id) in
                  let obsolete_fragments', loc' =
                    if Hashtbl.mem fragments loc_index
                    then begin
                        let fu,version = Hashtbl.find fragments loc_index in
                        let new_loc = (fu.FragmentUpdate.osd_id_o, version) in
                        ((loc_index, old_loc)::obsolete_fragments,
                         new_loc)
                    end else
                      (obsolete_fragments,
                       old_loc)
                  in
                  fragment_id + 1, loc' :: locs, obsolete_fragments')
               (0, [], obsolete_fragments)
               chunk_locations in
           chunk_id + 1,
           (List.rev locs_rev) :: locs,
           obsolete_fragments')
        (0, [], [])
        old_manifest_locations
    in
    let fragment_locations = List.rev locations_rev in

    let delete_keys_from_device =
      List.flatmap_unordered
        (function
          | (_,                       (None, _)) -> []
          | ((chunk_id, fragment_id), (Some osd_id, version_id)) ->
            let key_to_be_deleted1 =
              Osd_keys.AlbaInstance.fragment
                ~object_id
                ~version_id
                ~chunk_id
                ~fragment_id
              |> to_global_key
            in
            let key_to_be_deleted2 =
              Osd_keys.AlbaInstance.fragment_recovery_info
                ~object_id
                ~version_id
                ~chunk_id
                ~fragment_id
              |> to_global_key
            in
            List.map
              (fun k -> Update'.set ((Keys.Device.keys_to_be_deleted osd_id) ^ k) "")
              [ key_to_be_deleted1; key_to_be_deleted2 ])
        obsolete_fragments in

    let open Manifest in
    let open FragmentUpdate in
    let maybe_replace extract c i old  =
      try
        let key = (c,i) in
        let fu,_ = Hashtbl.find fragments key in
        match fu.size_change with
        | None -> old
        | Some x -> extract x
      with Not_found ->
        old
    in
    let new_fragment_checksums =
      Layout.map_indexed
        (fun c i f ->
          let old = Fragment.crc_of f in
          maybe_replace snd c i old)
        manifest_old.fragments
    in
    let new_fragment_packed_sizes =
      Layout.map_indexed
        (fun c i f ->
          let old = Fragment.len_of f in
          maybe_replace fst c i old)
        manifest_old.fragments
    in
    let new_fragment_ctrs =
      Layout.map_indexed
        (fun chunk_id fragment_id f ->
          let old = Fragment.ctr_of f in
          try
            let fu,_ =
              Hashtbl.find fragments (chunk_id, fragment_id)
            in
            fu.ctr
          with Not_found ->
            old)
        manifest_old.fragments
    in
    let fragments' =
      Layout.map4
        Fragment.make'
        fragment_locations
        new_fragment_checksums
        new_fragment_packed_sizes
        new_fragment_ctrs
    in
    let updated_manifest =
      { manifest_old with
        version_id;
        fragments = fragments';
      }
    in

    check_fragment_osd_spread updated_manifest;

    let device_obj_mapping_upds =
      update_device_object_mapping
        kv
        object_id
        ~old_locations:old_manifest_locations
        ~new_fragments:updated_manifest.fragments
        ~max_disks_per_node:updated_manifest.Manifest.max_disks_per_node
    in

    let update_storage_size =
      let storage_size_delta =
        let old_size = get_summed_fragment_sizes manifest_old
        and new_size = get_summed_fragment_sizes updated_manifest
        in
        Int64.sub new_size old_size
      in
      if storage_size_delta = 0L
      then []
      else
        [Update'.add Keys.namespace_storage_size storage_size_delta]
    in
    let update_buckets =
      let Storage_scheme.EncodeCompressEncrypt
          (Encoding_scheme.RSVM (k, m, _), _) =
        manifest_old.storage_scheme in

      let fragment_count_old, max_disks_per_node_old =
        get_min_fragment_count_and_max_disks_per_node
          kv
          ~k ~m
          manifest_old.fragments
          ~validate:false
      in
      let fragment_count_updated, max_disks_per_node_updated =
        let all_fragment_updates_are_removes =
          List.for_all
            (fun fu -> fu.osd_id_o = None)
            new_fragments
        in
        get_min_fragment_count_and_max_disks_per_node
          kv
          ~k ~m
          updated_manifest.fragments
          (* registering that a fragment was permanently lost
           * should always be possible, even if it violates the
           * policy.
           *)
          ~validate:(not all_fragment_updates_are_removes)
      in
      if fragment_count_old     <> fragment_count_updated ||
         max_disks_per_node_old <> max_disks_per_node_updated
      then
        [ Update'.delete
            (Keys.policies
               ~k ~m
               ~fragment_count:fragment_count_old
               ~max_disks_per_node:max_disks_per_node_old
               ~object_id);
          Update'.add
            (Keys.policies_cnt
               ~k ~m
               ~fragment_count:fragment_count_old
               ~max_disks_per_node:max_disks_per_node_old)
            (-1L);

          Update'.set
            (Keys.policies
               ~k ~m
               ~fragment_count:fragment_count_updated
               ~max_disks_per_node:max_disks_per_node_updated
               ~object_id)
            "";
          Update'.add
            (Keys.policies_cnt
               ~k ~m
               ~fragment_count:fragment_count_updated
               ~max_disks_per_node:max_disks_per_node_updated)
            1L;
        ]
      else
        []
    in

    let updated_manifest_s = serialize
                               (Manifest.to_buffer ~version:1)
                               updated_manifest
    in
    let update_manifest =
      Update'.compare_and_swap
        (Keys.objects ~object_id)
        (Some manifest_old_s)
        (Some updated_manifest_s) in
    List.concat
      [ delete_keys_from_device;
        update_manifest;
        [ assert_gc_epoch ];
        device_obj_mapping_upds;
        update_osd_sizes;
        update_buckets;
        update_storage_size;
      ]

  let update_manifest
        kv object_name object_id
        new_fragments gc_epoch version_id
    =
    let new_fragments' =
      List.map
        (fun (chunk_id, fragment_id, osd_id_o)
         -> FragmentUpdate.make
              chunk_id fragment_id  osd_id_o
              None None None
        )
        new_fragments
    in
    update_manifest_generic
      kv object_name object_id
      new_fragments' gc_epoch version_id

  let update_manifest2
        kv object_name object_id
        (new_fragments : FragmentUpdate.t list)
        gc_epoch
        version_id
    =
    update_manifest_generic
      kv object_name object_id
      new_fragments gc_epoch version_id

  let get_stats kv =
    let read key =
      let vo = KV.get kv key in
      match vo with
      | None -> 0L
      | Some v -> deserialize Llio.int64_from v
    in
    let logical_size = read Keys.namespace_logical_size in
    let storage_size = read Keys.namespace_storage_size in
    let bucket_count, _ =
      EKV.map_range
        kv
        ~first:(Keys.policies_cnt ~k:0 ~m:0 ~fragment_count:0 ~max_disks_per_node:0) ~finc:true
        ~last:Keys.policies_cnt_next_prefix
        ~max:(-1) ~reverse:false
        (fun cur key ->
           let k, m, fragment_count, max_disks_per_node = Keys.parse_policies_cnt key in
           let cnt = deserialize Llio.int64_from (KV.cur_get_value cur) in
           ((Int32.to_int k, Int32.to_int m,
             Int32.to_int fragment_count, Int32.to_int max_disks_per_node),
            cnt))
    in
    let storage_size_per_osd, _ =
      EKV.map_range
        kv
        ~first:Keys.Device.size_prefix ~finc:true
        ~last:Keys.Device.size_next_prefix
        ~max:(-1) ~reverse:false
        (fun cur key ->
           let osd_id = Keys.Device.size_extract_osd_id key in
           let storage = deserialize Llio.int64_from (KV.cur_get_value cur) in
           osd_id, storage)
    in
    NamespaceStats.({
        logical_size;
        storage_size;
        storage_size_per_osd;
        bucket_count;
      })



  let list_objects_by_policy kv ~first ~finc ~last ~max ~reverse =
    EKV.map_range
      kv
      ~first:(let (k, m, fragment_count, max_disks_per_node), object_id = first in
              Keys.policies ~k ~m ~fragment_count ~max_disks_per_node ~object_id)
      ~finc
      ~last:(match last with
          | None -> Keys.policies_next_prefix
          | Some (((k, m, fragment_count, max_disks_per_node), object_id),
                  linc) ->
            Some (Keys.policies ~k ~m ~fragment_count ~max_disks_per_node ~object_id, linc))
      ~max ~reverse
      (fun cur key ->
         let k, m, fragment_count, max_disks_per_node, object_id = Keys.parse_policies_key key in
         let manifest, _ = get_object_manifest_by_id kv object_id in
         manifest)

  let update_preset kv preset version =
    let version_s = KV.get kv Keys.preset_version in
    let version' =
      match version_s with
      | None -> -1L
      | Some v -> deserialize Llio.int64_from v
    in
    if version > version'
    then
      begin
        [ Update'.Assert (Keys.preset_version,
                          version_s);
          Update'.set Keys.preset_version
                      (serialize Llio.int64_to version);
          Update'.set Keys.preset
                      (serialize (Preset.to_buffer ~version:2) preset); ],
        ()
      end
    else
      [], ()

(*

TODO don't reverse lists just to serialize them (be careful with Llio.list functions!)

TODO manifest representation should be updateable

TODO in general all representations should be updateable!

TODO only do string concatenation in Keys module

TODO add (snappy) compression to protocol?

*)

  (*
TODO add support for adding object which could only be partially uploaded and are in need of repair?
(or at least make sure schema is easily evolvable to support this)
*)

end
