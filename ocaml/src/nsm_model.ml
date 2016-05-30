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
open Key_value_store

type k = Policy.k [@@deriving show]
type m = Policy.m [@@deriving show]

type object_name = string [@@deriving show]
type object_id = HexString.t [@@deriving show]

module OsdInfo = struct
  type long_id = string [@@deriving show, yojson]

  type ip = string [@@deriving show, yojson]
  type port = int [@@deriving show, yojson]

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

  type kind =
    | Asd     of conn_info * asd_id
    | Kinetic of conn_info * kinetic_id
    | Alba    of alba_cfg
                   [@@deriving show, yojson]

  let get_long_id = function
    | Asd (_, asd_id)         -> asd_id
    | Kinetic (_, kinetic_id) -> kinetic_id
    | Alba { id; }            -> id

  let get_conn_info = function
    | Asd     (info, _)
    | Kinetic (info, _) ->
       info
    (* | Alba (info, _) -> *)
    (*    `Y info *)

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
  }
  [@@deriving show, yojson]

  let make
      ~kind ~node_id
      ~decommissioned ~other
      ~total ~used
      ~seen ~read ~write ~errors
    =
    { kind; node_id;
      decommissioned; other;
      total; used;
      seen; read; write; errors; }

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
        errors
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
    }

  let _from_buffer2 orig_buf =
    let bufs = Llio.string_from orig_buf in
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
    }

  let _from_buffer3 orig_buf =
    let bufs = Llio.string_from orig_buf in
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
    }


  let from_buffer buf =
    let ser_version = Llio.int8_from buf in
    match ser_version with
    | 3 -> _from_buffer3 buf
    | 2 -> _from_buffer2 buf
    | 1 -> _from_buffer1 buf
    | k -> raise_bad_tag "OsdInfo.ser_version" k



end

type osd_id = int32 [@@deriving show, yojson]

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

type chunk_size = int
[@@deriving show]

module EncryptInfo = struct
  open Encryption

  type key_identification =
    | KeySha1 of string
  [@@deriving show]

  let id_to_buffer buf = function
    | KeySha1 id ->
      Llio.int8_to buf 1;
      Llio.string_to buf id

  let id_from_buffer buf =
    let k = Llio.int8_from buf in
    if k <> 1
    then raise_bad_tag "EncryptInfo.key_identification" k;
    KeySha1 (Llio.string_from buf)

  type t =
    | NoEncryption
    | Encrypted of Encryption.algo * key_identification
  [@@deriving show]

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
  [@@deriving show]

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



module Layout = struct
    type 'a t = 'a list list [@@deriving show]
    let map f t = List.map (List.map f) t
    let output a_to buf t =
      let ser_version = 1 in Llio.int8_to buf ser_version;
      Llio.list_to (Llio.list_to a_to) buf t
    let input a_from buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      Llio.list_from (Llio.list_from a_from) buf

    let combine x_t y_t = List.map2 List.combine x_t y_t

    let split xy_t = List.split (List.map List.split xy_t)

    let congruent x_t y_t =
      let list_ok x y = List.length x = List.length y in
      let rec ok = function
        | [],[] -> true
        | x :: xs, y::ys -> list_ok x y && ok (xs,ys)
        | _,_ -> false
      in
      ok (x_t,y_t)

    let index t chunk_id fragment_id =
      List.nth_exn (List.nth_exn t chunk_id) fragment_id
end

type version = int [@@deriving show]
type location = osd_id option * version [@@deriving show]

type chunk_id = int [@@deriving show]
(* 0 <= fragment_id < k+m *)
type fragment_id = int [@@deriving show]

include Checksum

module DeviceSet = Set.Make(struct type t = osd_id let compare = compare end)

module Manifest = struct

  type t = {
    name : string;
    object_id : string;

    storage_scheme : Storage_scheme.t;
    encrypt_info : EncryptInfo.t;

    chunk_sizes : int list;
    size : Int64.t; (* size of the object *)
    checksum : Checksum.t;

    fragment_locations : location Layout.t;
    fragment_checksums : Checksum.t Layout.t;
    fragment_packed_sizes : int Layout.t;
    version_id : version;
    max_disks_per_node : int;

    timestamp : float;
  }
  [@@deriving show]

  let make ~name ~object_id
           ~storage_scheme ~encrypt_info
           ~chunk_sizes ~size
           ~checksum
           ~fragment_locations
           ~fragment_checksums
           ~fragment_packed_sizes
           ~version_id
           ~max_disks_per_node
           ~timestamp
    =
    { name; object_id;
      storage_scheme; encrypt_info;
      chunk_sizes; checksum; size;
      fragment_locations;
      fragment_checksums;
      fragment_packed_sizes;
      version_id;
      max_disks_per_node;
      timestamp;
    }

  let get_summed_fragment_sizes t =
    List.fold_left
      (fun acc l ->
         List.fold_left
           (fun acc s -> Int64.(add acc (of_int s)))
           acc
           l)
      0L
      t.fragment_packed_sizes

  let to_buffer' buf t =
    Llio.string_to buf t.name;
    Llio.string_to buf t.object_id;
    Llio.list_to Llio.int_to buf t.chunk_sizes;
    Storage_scheme.output buf t.storage_scheme;
    EncryptInfo.to_buffer buf t.encrypt_info;
    Checksum.output buf t.checksum;
    Llio.int64_to buf t.size;
    Layout.output
      (Llio.pair_to
         (Llio.option_to
            Llio.int32_to)
         Llio.int_to)
      buf t.fragment_locations;
    Layout.output Checksum.output buf t.fragment_checksums;
    Layout.output Llio.int_to buf t.fragment_packed_sizes;
    Llio.int_to buf t.version_id;
    Llio.int_to buf t.max_disks_per_node;
    Llio.float_to buf t.timestamp

  let to_buffer buf t =
    let res = serialize to_buffer' t in
    let ser_version = 1 in
    Llio.int8_to buf ser_version;
    Llio.string_to buf (Snappy.compress res)

  let output = to_buffer

  let from_buffer' buf =
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
           (Llio.option_from
              Llio.int32_from)
           Llio.int_from)
        buf in
    let fragment_checksums = Layout.input Checksum.input buf in
    let fragment_packed_sizes = Layout.input Llio.int_from buf in
    let version_id = Llio.int_from buf in
    let max_disks_per_node = Llio.int_from buf in
    let timestamp = Llio.float_from buf in
    make ~name ~object_id
         ~storage_scheme ~encrypt_info
         ~chunk_sizes ~checksum ~size
         ~fragment_locations
         ~fragment_checksums
         ~fragment_packed_sizes
         ~version_id
         ~max_disks_per_node
         ~timestamp

  let from_buffer buf =
    match Llio.int8_from buf with
    | 1 ->
      let s = Snappy.uncompress (Llio.string_from buf) in
      deserialize from_buffer' s
    | k -> raise_bad_tag "Nsm_model.Manifest" k

  let input = from_buffer

  let osds_used fragment_locations =
    DeviceSet.of_list
      (List.flatmap_unordered
         (List.map_filter_rev fst)
         fragment_locations)
end

module Assert =
  struct
    type t =
      | ObjectExists of object_name
      | ObjectDoesNotExist of object_name
      | ObjectHasId of object_name * object_id
    (* | ObjectHasChecksum ? *)

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
      | k -> raise_bad_tag "Nsm_model.Assert" k
  end

module Update =
  struct
    type t =
      | PutObject of Manifest.t * GcEpochs.gc_epoch
      | DeleteObject of object_name

    let to_buffer buf = function
      | PutObject (mf, gc_epoch) ->
         Llio.int8_to buf 1;
         Manifest.to_buffer buf mf;
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
         Llio.int32_to
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
           Llio.int32_from
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

  module Device = struct

    let s device_id = serialize Llio.int32_be_to device_id

    let extract_osd_id_from ~prefix_len =
      deserialize
        ~offset:prefix_len
        Llio.int32_be_from

    let info ~osd_id = "osds/info/" ^ (s osd_id)

    let active_osds_prefix = "osds/active/"
    let active_osds_next_prefix = next_prefix active_osds_prefix
    let active_osds ~osd_id = active_osds_prefix ^ (s osd_id)
    let active_osds_extract_osd_id =
      let prefix_len = String.length active_osds_prefix in
      extract_osd_id_from ~prefix_len

    (* listing of all objects that have fragments on this device, to be used during rebuild/repair *)
    let objects_prefix device_id =
      Printf.sprintf "osds/objects/%s/" (s device_id)
    let objects device_id object_id =
      (objects_prefix device_id) ^ object_id
    let objects_next_prefix device_id =
      next_prefix (objects_prefix device_id)
    let objects_extract_object_id =
      let prefix_len = String.length (objects_prefix 0l) in
      fun key -> Str.string_after key prefix_len

    (* set of keys still to be deleted from a device *)
    let keys_to_be_deleted device_id =
      Printf.sprintf "osds/deletes/%s/" (s device_id)
    let keys_to_be_deleted_next_prefix device_id =
      next_prefix (Printf.sprintf "osds/deletes/%s" (s device_id))
    let keys_to_be_deleted_extract_key =
      let prefix_len = String.length (keys_to_be_deleted 0l) in
      fun key -> Str.string_after key prefix_len

    let size_prefix = "osds/size/"
    let size osd_id = Printf.sprintf "%s%s" size_prefix (s osd_id)
    let size_next_prefix = next_prefix size_prefix
    let size_extract_osd_id =
      let prefix_len = String.length size_prefix in
      extract_osd_id_from ~prefix_len
  end
end

module type Constants = sig
  val namespace_id : int32
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
  [@@deriving show, enum]

  exception Nsm_exn of t * string

  let failwith ?(payload="") err = raise (Nsm_exn (err, payload))

  let err2int = to_enum
  let int2err x = Option.get_some_default Unknown (of_enum x)
end


let check_fragment_osd_spread manifest =
  List.iter
    (fun locations ->
       (* for each chunk check if all fragments are stored on different osds *)
       let (_ : DeviceSet.t) = List.fold_left
           (fun acc -> function
              | None, _ -> acc
              | Some osd_id, _ ->
                if DeviceSet.mem osd_id acc
                then Err.(failwith
                            ~payload:([%show : (int32 option * int) list] locations)
                            Invalid_fragment_spread)
                else DeviceSet.add osd_id acc)
           DeviceSet.empty
           locations in
       ())
    manifest.Manifest.fragment_locations

module Update' = Key_value_store.Update

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
             (fun a -> function
                | (_,             (None, _)) -> a
                | (fragment_size, (Some osd_id, _)) ->
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
      (Layout.combine
         manifest.Manifest.fragment_packed_sizes
         manifest.Manifest.fragment_locations)


  let get_min_fragment_count_and_max_disks_per_node kv ~k ~max_disks_per_node locations =
    let get_bla_per_chunk chunk_location =
      let osds_per_node = Hashtbl.create 3 in
      List.fold_left
        (fun (effective_fragment_count, effective_max_disks_per_node) -> function
           | None, _ ->
             (effective_fragment_count,
              effective_max_disks_per_node)
           | Some osd_id, _ ->
             let osd_info = get_osd_info kv osd_id in
             let node_id = osd_info.OsdInfo.node_id in
             let cnt =
               try Hashtbl.find osds_per_node node_id
               with Not_found -> 0
             in

             let cnt' = cnt + 1 in
             if cnt' > max_disks_per_node
             then Err.(failwith
                         ~payload:(Printf.sprintf
                                     "Attempting to use %i disks of node %s while max %i permitted (%s)"
                                     cnt' node_id max_disks_per_node
                                     ([%show : int32 option list]
                                        (List.map fst chunk_location)))
                         Too_many_disks_per_node);

             Hashtbl.replace osds_per_node node_id cnt';
             (effective_fragment_count + 1,
              max effective_max_disks_per_node cnt'))
        (0, 0)
        chunk_location
    in
    let min_fragment_count, max_disks_per_node =
      List.fold_left
        (fun (effective_fragment_count, effective_max_disks_per_node) chunk_location ->
           let effective_fragment_count', effective_max_disks_per_node' =
             get_bla_per_chunk chunk_location
           in
           (min effective_fragment_count effective_fragment_count',
            max effective_max_disks_per_node effective_max_disks_per_node'))
        (max_int, 0)
        locations
    in

    (* TODO should actually compare with whatever is specified in the policy *)
    if min_fragment_count < k
    then Err.(failwith Insufficient_fragments);

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
    let from_chunk_locations chunk_id chunk_locations =
      List.fold_left
        (fun (fragment_id, deletes) location ->
           let deletes' = match location with
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
        chunk_locations in
    let _, delete_fragments_and_recovery_info =
      List.fold_left
        (fun (chunk_id, acc_delete_fragments) chunk_locations ->
           let _, delete_fragments = from_chunk_locations chunk_id chunk_locations in
           chunk_id + 1,
           List.rev_append delete_fragments acc_delete_fragments)
        (0, [])
        old_manifest.fragment_locations
    in
    let update_osd_sizes = get_osd_size_updates ~delete:true old_manifest in

    let delete_from_device_objects =
      let old_devices = Manifest.osds_used old_manifest.fragment_locations in
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
          ~k ~max_disks_per_node:old_manifest.max_disks_per_node
          old_manifest.fragment_locations
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
      ~old_locations ~new_locations
      ~max_disks_per_node =

    (* in old manifest there is a set of devices, in the updated manifest there
       is a different set, so the mapping containing the objects on each disk
       has to be updated
       S1 \ S2 -> some keys to be removed
       S2 \ S1 -> some keys to be added *)
    let old_devices = Manifest.osds_used old_locations in
    let new_devices = Manifest.osds_used new_locations in

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
           then Err.(failwith ~payload:(Int32.to_string osd_id) Inactive_osd);

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
        (Some (serialize Manifest.output manifest)) in

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
         if manifest.Manifest.timestamp <= old_timestamp
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
        ~new_locations:manifest.fragment_locations
        ~max_disks_per_node:manifest.Manifest.max_disks_per_node
    in
    let add_policy =
      let Storage_scheme.EncodeCompressEncrypt
          (Encoding_scheme.RSVM (k, m, _), _) =
        manifest.storage_scheme in
      let fragment_count, max_disks_per_node =
        get_min_fragment_count_and_max_disks_per_node
          kv
          ~k ~max_disks_per_node:manifest.max_disks_per_node
          manifest.fragment_locations
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
          ~old_locations:manifest.Manifest.fragment_locations
          ~new_locations:[]
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
         (* TODO
          * mss willen er op dezelfde keys werken, via assert&set ipv user function
          *  ... dan is't een probleem
          * voorlopig minstens sanity check inbouwen dat niet 2 keer
          * dezelfde key gemanipuleerd wordt?
          *)
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

  let update_manifest
      kv object_name object_id
      (new_fragments : (chunk_id * fragment_id * osd_id option) list)
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
      (fun (chunk_id, fragment_id, osd_id_o) ->
         Hashtbl.add
           fragments
           (chunk_id, fragment_id)
           (osd_id_o, version_id))
      new_fragments;
    let manifest_old, manifest_old_s = get_object_manifest_by_id kv object_id in

    assert (object_name = manifest_old.Manifest.name);

    if (manifest_old.Manifest.version_id + 1) <> version_id
    then Err.failwith Err.InvalidVersionId;

    let old_manifest_locations = manifest_old.Manifest.fragment_locations in

    let update_osd_sizes =
      List.fold_left
        (fun acc (chunk_id, fragment_id, osd_id_o) ->
           let fragment_size =
             Layout.index
               manifest_old.Manifest.fragment_packed_sizes
               chunk_id
               fragment_id
           in
           let upd1 = match osd_id_o with
             | None -> []
             | Some osd_id ->
               [ Update'.add
                   (Keys.Device.size osd_id)
                   (Int64.of_int fragment_size) ]
           in
           let upd2 =
             match Layout.index
                     old_manifest_locations
                     chunk_id
                     fragment_id with
             | None, _ -> []
             | Some previous_osd_id, _ ->
               [ Update'.add
                   (Keys.Device.size previous_osd_id)
                   (Int64.of_int (-fragment_size)) ]
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
                      let new_loc = Hashtbl.find fragments loc_index in
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
    let updated_manifest =
      { manifest_old with
        version_id;
        fragment_locations;
      }
    in

    check_fragment_osd_spread updated_manifest;

    let device_obj_mapping_upds =
      update_device_object_mapping
        kv
        object_id
        ~old_locations:(manifest_old.Manifest.fragment_locations)
        ~new_locations:(updated_manifest.Manifest.fragment_locations)
        ~max_disks_per_node:updated_manifest.Manifest.max_disks_per_node
    in

    let update_buckets =
      let Storage_scheme.EncodeCompressEncrypt
          (Encoding_scheme.RSVM (k, m, _), _) =
        manifest_old.storage_scheme in
      let max_disks_per_node = manifest_old.max_disks_per_node in

      let fragment_count_old, max_disks_per_node_old =
        get_min_fragment_count_and_max_disks_per_node
          kv ~k ~max_disks_per_node
          manifest_old.fragment_locations
      in
      let fragment_count_updated, max_disks_per_node_updated =
        get_min_fragment_count_and_max_disks_per_node
          kv ~k ~max_disks_per_node
          updated_manifest.fragment_locations
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

    let updated_manifest_s = serialize Manifest.output updated_manifest in
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
      ]


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
