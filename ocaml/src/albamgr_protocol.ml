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
open Statistics_collection

module Protocol = struct

  type alba_id = string



  module Nsm_host = struct
    (* in theory this could be anything,
       in practice it will be the arakoon cluster_id *)
    type id = string [@@deriving show, yojson]

    type kind =
      | Arakoon of Alba_arakoon.Config.t
    [@@deriving show]

    type t = {
      kind : kind;
      lost : bool;
    }
    [@@deriving show]

    let to_buffer buf t =
      Llio.bool_to buf t.lost;
      match t.kind with
      | Arakoon cfg ->
        Llio.int8_to buf 1;
        Alba_arakoon.Config.to_buffer buf cfg

    let from_buffer buf =
      let lost = Llio.bool_from buf in
      let kind = match Llio.int8_from buf with
        | 1 -> Arakoon (Alba_arakoon.Config.from_buffer buf)
        | k -> raise_bad_tag "Nsm_host" k
      in
      { lost; kind; }
  end

  module Namespace = struct
    type name = Nsm_host_protocol.Protocol.namespace_name [@@deriving show, yojson]
    type id = Nsm_host_protocol.Protocol.namespace_id [@@deriving show, yojson]

    type state =
      | Creating
      | Active
      | Removing
      | Recovering
    [@@deriving show]

    type t = {
      id : id;
      nsm_host_id : Nsm_host.id;
      state : state;
      preset_name : string;
    } [@@deriving show]

    let to_buffer buf t =
      let ser_version = 1 in Llio.int8_to buf ser_version;
      Llio.int32_to buf t.id;
      Llio.string_to buf t.nsm_host_id;
      Llio.int8_to
        buf
        (match t.state with
         | Creating -> 1
         | Active -> 2
         | Removing -> 3
         | Recovering -> 4);
      Llio.string_to buf t.preset_name

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let id = Llio.int32_from buf in
      let nsm_host_id = Llio.string_from buf in
      let state =
        match Llio.int8_from buf with
        | 1 -> Creating
        | 2 -> Active
        | 3 -> Removing
        | 4 -> Recovering
        | k -> raise_bad_tag "Namespace" k
      in
      let preset_name = Llio.string_from buf in
      { id; nsm_host_id; state; preset_name; }

    let deser = from_buffer, to_buffer
  end

  module Osd = struct

    type id = Nsm_model.osd_id [@@deriving show, yojson]

    module Update = struct
      open Nsm_model.OsdInfo
      type t = {
        (* asd/kinetic *)
        ips' : ip list option;
        port' : port option;
        (* alba osd *)
        albamgr_cfg' : Alba_arakoon.Config.t option;

        total' : int64 option;
        used' : int64 option;
        seen' : float list;
        read' : float list;
        write' : float list;
        errors' : (float * string) list;
        other' : string option;
      } [@@deriving show]

      let make
        ?ips' ?port'
        ?total' ?used'
        ?(seen' = []) ?(read' = []) ?(write' = []) ?(errors' = [])
        ?other' ?albamgr_cfg' () =
        { ips'; port';
          albamgr_cfg';
          total'; used';
          seen'; read'; write'; errors';
          other';
        }

      let apply
          osd
          { ips'; port';
            albamgr_cfg';
            total'; used';
            seen'; read'; write'; errors';
            other';
          }
        =
        let my_compare x y = compare y x in
        let kind =
          let get_conn_info' conn_info =
            let ips, port, tls, use_rdma = conn_info in
            let conn_info' =
              let ips'  = Option.get_some_default ips ips' in
              let port' = Option.get_some_default port port' in
              let real_port, use_tls, use_rdma' =
                let open Tiny_json in
                let extract_port_tls r =
                  try
                    Json.getf "tlsPort" r |> Json.as_int, true
                  with
                  | Json.JSON_InvalidField("tlsPort") -> port', false
                in
                let extract_rdma r =
                  try
                    Json.getf "useRdma" r |> Json.as_bool
                  with
                  | Json.JSON_InvalidField("useRdma") -> false
                in
                match other' with
                | None -> port', tls, use_rdma
                | Some other' ->
                   try
                     let r = Json.parse other' in
                     let real_port, use_tls = extract_port_tls r in
                     let use_rdma' = extract_rdma r in
                     real_port, use_tls,use_rdma'
                   with
                   | ex ->
                      let () = Plugin_helper.debug_f "ex:%s" (Printexc.to_string ex) in
                      port', false, use_rdma
              in
              (ips',real_port, use_tls, use_rdma')
            in
            conn_info'
          in
          let get_cfg cfg =
            match albamgr_cfg' with
            | None -> cfg
            | Some cfg' ->
               (* cluster_id shouldn't change! *)
               let open Arakoon_client_config in
               assert (cfg.cluster_id = cfg'.cluster_id);
               cfg'
          in
          match osd.kind with
          | Asd (conn_info, asd_id)   -> Asd (get_conn_info' conn_info, asd_id)
          | Kinetic (conn_info, k_id) -> Kinetic (get_conn_info' conn_info, k_id)
          | Alba  ({ cfg; _ } as x) -> Alba
                                         { x with
                                           cfg = get_cfg cfg;
                                         }
          | Alba2 ({ cfg; _ } as x) -> Alba2
                                         { x with
                                           cfg = get_cfg cfg;
                                         }
        in
        let max_n = 10 in
        { node_id = osd.node_id;
          kind; decommissioned = osd.decommissioned;
          other = Option.get_some_default osd.other other';
          total = Option.get_some_default osd.total total';
          used = Option.get_some_default osd.used used';
          seen   = List.merge_head  ~compare:my_compare osd.seen seen' max_n;
          read   = List.merge_head  ~compare:my_compare osd.read read' max_n;
          write  = List.merge_head  ~compare:my_compare osd.write write' max_n;
          errors = List.merge_head
                     ~compare:(fun i1 i2 -> my_compare (fst i1) (fst i2))
                     osd.errors errors' max_n;
        }

      let _to_buffer_1 buf u =
        Llio.option_to (Llio.list_to Llio.string_to) buf u.ips';
        Llio.option_to Llio.int_to buf u.port';
        Llio.option_to Llio.int64_to buf u.total';
        Llio.option_to Llio.int64_to buf u.used';
        Llio.list_to Llio.float_to buf u.seen';
        Llio.list_to Llio.float_to buf u.read';
        Llio.list_to Llio.float_to buf u.write';
        Llio.list_to
          (Llio.pair_to
             Llio.float_to
             Llio.string_to)
          buf
          u.errors';
        Llio.option_to Llio.string_to buf u.other'

      let _to_buffer_2 buf u =
        let s =
          let buf = Buffer.create 128 in
          Llio.option_to (Llio.list_to Llio.string_to) buf u.ips';
          Llio.option_to Llio.int_to buf u.port';
          Llio.option_to Llio.int64_to buf u.total';
          Llio.option_to Llio.int64_to buf u.used';
          Llio.list_to Llio.float_to buf u.seen';
          Llio.list_to Llio.float_to buf u.read';
          Llio.list_to Llio.float_to buf u.write';
          Llio.list_to
            (Llio.pair_to
               Llio.float_to
               Llio.string_to)
            buf
            u.errors';
          Llio.option_to Llio.string_to buf u.other';
          Llio.option_to Alba_arakoon.Config.to_buffer buf u.albamgr_cfg';
          Buffer.contents buf
        in
        Llio.string_to buf s

      let to_buffer buf u ~version =
        Llio.int8_to buf version;
        match version with
        | 2 -> _to_buffer_2 buf u
        | 1 -> _to_buffer_1 buf u
        | _ -> assert false

      let _from_buffer_1 buf =
        let ips' =
          Llio.option_from
            (Llio.list_from Llio.string_from) buf in
        let port' = Llio.option_from Llio.int_from buf in
        let total' = Llio.option_from Llio.int64_from buf in
        let used' = Llio.option_from Llio.int64_from buf in
        let seen' = Llio.list_from Llio.float_from buf in
        let read' = Llio.list_from Llio.float_from buf in
        let write' = Llio.list_from Llio.float_from buf in
        let errors' =
          Llio.list_from
            (Llio.pair_from
               Llio.float_from
               Llio.string_from)
            buf in
        let other' = Llio.option_from Llio.string_from buf in
        { ips'; port';
          albamgr_cfg' = None;
          total'; used';
          seen'; read'; write'; errors';
          other';
        }

      let _from_buffer_2 buf =
        let bufs = Llio.string_from buf in
        let buf = Llio.make_buffer bufs 0 in
        let ips' =
          Llio.option_from
            (Llio.list_from Llio.string_from) buf in
        let port' = Llio.option_from Llio.int_from buf in
        let total' = Llio.option_from Llio.int64_from buf in
        let used' = Llio.option_from Llio.int64_from buf in
        let seen' = Llio.list_from Llio.float_from buf in
        let read' = Llio.list_from Llio.float_from buf in
        let write' = Llio.list_from Llio.float_from buf in
        let errors' =
          Llio.list_from
            (Llio.pair_from
               Llio.float_from
               Llio.string_from)
            buf in
        let other' = Llio.option_from Llio.string_from buf in
        let albamgr_cfg' = Llio.option_from Alba_arakoon.Config.from_buffer buf in
        { ips'; port';
          albamgr_cfg';
          total'; used';
          seen'; read'; write'; errors';
          other';
        }

      let from_buffer buf =
        let ser_version = Llio.int8_from buf in
        match ser_version with
        | 1 -> _from_buffer_1 buf
        | 2 -> _from_buffer_2 buf
        | k -> raise_bad_tag "Albamgr_protocol.Osd.Update" k
    end

    module ClaimInfo = struct
      type t =
        | ThisAlba of id
        | AnotherAlba of string
        | Available
      [@@deriving show]

      let to_buffer buf = function
        | ThisAlba osd_id ->
          Llio.int8_to buf 1;
          Llio.int32_to buf osd_id
        | AnotherAlba alba ->
          Llio.int8_to buf 2;
          Llio.string_to buf alba
        | Available ->
          Llio.int8_to buf 3

      let from_buffer buf =
        match Llio.int8_from buf with
        | 1 -> ThisAlba (Llio.int32_from buf)
        | 2 -> AnotherAlba (Llio.string_from buf)
        | 3 -> Available
        | k -> raise_bad_tag "Osd.ClaimInfo" k

      let deser = from_buffer,to_buffer
    end


    let from_buffer_with_claim_info =
      Llio.pair_from ClaimInfo.from_buffer Nsm_model.OsdInfo.from_buffer
    let to_buffer_with_claim_info ~version =
      Llio.pair_to ClaimInfo.to_buffer (Nsm_model.OsdInfo.to_buffer ~version)

    module Message = struct
      type t =
        | AddNamespace of Namespace.name * Namespace.id
      [@@ deriving show]

      let from_buffer buf =
        match Llio.int8_from buf with
        | 1 ->
          let name = Llio.string_from buf in
          let id = Llio.int32_from buf in
          AddNamespace (name, id)
        | k -> raise_bad_tag "Osd.Message" k

      let to_buffer buf = function
        | AddNamespace (name, id) ->
          Llio.int8_to buf 1;
          Llio.string_to buf name;
          Llio.int32_to buf id
    end

    module NamespaceLink = struct
      type state =
        | Adding
        | Active
        | Decommissioning
        | Repairing
      [@@ deriving show]

      let from_buffer buf =
        match Llio.int8_from buf with
        | 1 -> Adding
        | 2 -> Active
        | 3 -> Decommissioning
        | 4 -> Repairing
        | k -> raise_bad_tag "Osd.NamespaceLink" k

      let to_buffer buf = function
        | Adding -> Llio.int8_to buf 1
        | Active -> Llio.int8_to buf 2
        | Decommissioning -> Llio.int8_to buf 3
        | Repairing -> Llio.int8_to buf 4
    end
  end

  module Msg_log = struct
    type id = int32

    type ('dest, 'msg) t =
      | Nsm_host : (Nsm_host.id, Nsm_host_protocol.Protocol.Message.t) t
      | Osd : (Osd.id, Osd.Message.t) t

    type t' = Wrap : _ t -> t'

    let from_buffer buf : t' =
      match Llio.int8_from buf with
      | 1 -> Wrap Nsm_host
      | 2 -> Wrap Osd
      | k -> raise_bad_tag "Msg_log.t" k
    let to_buffer buf = function
      | Wrap Nsm_host -> Llio.int8_to buf 1
      | Wrap Osd -> Llio.int8_to buf 2

    let dest_from_buffer : type dest msg. (dest, msg) t -> dest Llio.deserializer = function
      | Nsm_host -> Llio.string_from
      | Osd -> Llio.int32_from
    let dest_to_buffer : type dest msg. (dest, msg) t -> dest Llio.serializer = function
      | Nsm_host -> Llio.string_to
      | Osd -> Llio.int32_to

    let msg_from_buffer : type dest msg. (dest, msg) t -> msg Llio.deserializer = function
      | Nsm_host -> Nsm_host_protocol.Protocol.Message.from_buffer
      | Osd -> Osd.Message.from_buffer
    let msg_to_buffer : type dest msg. (dest, msg) t -> msg Llio.serializer = function
      | Nsm_host -> Nsm_host_protocol.Protocol.Message.to_buffer
      | Osd -> Osd.Message.to_buffer
  end

  module Preset = struct
    type osds =
      | All
      | Explicit of Osd.id list
    [@@deriving show]

    let osds_to_buffer buf = function
      | All -> Llio.int8_to buf 1
      | Explicit osd_ids ->
        Llio.int8_to buf 2;
        Llio.list_to Llio.int32_to buf osd_ids

    let osds_from_buffer buf =
      match Llio.int8_from buf with
      | 1 -> All
      | 2 -> Explicit (Llio.list_from Llio.int32_from buf)
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
      w : Nsm_model.Encoding_scheme.w;
      policies : Policy.policy list;
      fragment_size : int;
      osds : osds;
      compression : Alba_compression.Compression.t;
      object_checksum : object_checksum;
      fragment_checksum_algo : Checksum.Checksum.algo;
      fragment_encryption : Encryption.Encryption.t;
    }
    [@@deriving show]


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
      default_in_allowed_list && enc_key_length && policies_ok

    type name = string [@@deriving show]

    let to_buffer buf t =
      let ser_version = 1 in Llio.int8_to buf ser_version;
      Nsm_model.Encoding_scheme.w_to_buffer buf t.w;
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

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let w = Nsm_model.Encoding_scheme.w_from_buffer buf in
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


    let _DEFAULT = {
        policies = [(5, 4, 8, 3); (2, 2, 3, 4);];
        w = Nsm_model.Encoding_scheme.W8;
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
  end

  module Work = struct
    type id = Int64.t

    type verify_params = {
        checksum : bool;
        repair_osd_unavailable : bool;
      } [@@deriving show]
    type action =
      | Rewrite
      | Verify of verify_params
    [@@deriving show]
    let action_to_buffer buf = function
      | Rewrite -> Llio.int8_to buf 1
      | Verify { checksum; repair_osd_unavailable; } ->
         Llio.int8_to buf 2;
         Llio.bool_to buf checksum;
         Llio.bool_to buf repair_osd_unavailable
    let action_from_buffer buf =
      match Llio.int8_from buf with
      | 1 -> Rewrite
      | 2 ->
         let checksum = Llio.bool_from buf in
         let repair_osd_unavailable = Llio.bool_from buf in
         Verify { checksum; repair_osd_unavailable; }
      | k -> raise_bad_tag "Work.action" k

    type range = string * string option [@@deriving show]

    type t =
      | CleanupNsmHostNamespace of Nsm_host.id * Namespace.id
      | CleanupOsdNamespace of Osd.id * Namespace.id
      | CleanupNamespaceOsd of Namespace.id * Osd.id
      | RepairBadFragment of
          Namespace.id * Nsm_model.object_id * Nsm_model.object_name *
          Nsm_model.chunk_id * Nsm_model.fragment_id * Nsm_model.version
      | RewriteObject of Namespace.id * Nsm_model.object_id
      | WaitUntilRepaired of Osd.id * Namespace.id
      | WaitUntilDecommissioned of Osd.id
      | IterNamespace of action * Namespace.id * string * int
      | IterNamespaceLeaf of action * Namespace.id * string * range
    [@@ deriving show]

    let to_buffer buf = function
      | CleanupNsmHostNamespace (nsm_host_id, namespace_id) ->
        Llio.int8_to buf 1;
        Llio.string_to buf nsm_host_id;
        Llio.int32_to buf namespace_id
      | CleanupOsdNamespace (osd_id, namespace_id) ->
        Llio.int8_to buf 2;
        Llio.int32_to buf osd_id;
        Llio.int32_to buf namespace_id
      | CleanupNamespaceOsd (namespace_id, osd_id) ->
        Llio.int8_to buf 3;
        Llio.int32_to buf namespace_id;
        Llio.int32_to buf osd_id
      | RepairBadFragment (namespace_id, object_id, object_name,
                           chunk_id, fragment_id, version) ->
        Llio.int8_to buf 6;
        Llio.int32_to buf namespace_id;
        Llio.string_to buf object_id;
        Llio.string_to buf object_name;
        Llio.int_to buf chunk_id;
        Llio.int_to buf fragment_id;
        Llio.int_to buf version
      | RewriteObject (namespace_id, object_id) ->
        Llio.int8_to buf 13;
        Llio.int32_to buf namespace_id;
        Llio.string_to buf object_id
      | WaitUntilRepaired (osd_id, namespace_id) ->
        Llio.int8_to buf 8;
        Llio.int32_to buf osd_id;
        Llio.int32_to buf namespace_id
      | WaitUntilDecommissioned osd_id ->
        Llio.int8_to buf 10;
        Llio.int32_to buf osd_id
      | IterNamespace (action, namespace_id, name, cnt) ->
        Llio.int8_to buf 11;
        action_to_buffer buf action;
        Llio.int32_to buf namespace_id;
        Llio.string_to buf name;
        Llio.int_to buf cnt
      | IterNamespaceLeaf (action, namespace_id, name, range) ->
        Llio.int8_to buf 12;
        action_to_buffer buf action;
        Llio.int32_to buf namespace_id;
        Llio.string_to buf name;
        Llio.pair_to
          Llio.string_to
          (Llio.option_to Llio.string_to)
          buf
          range

    let from_buffer buf =
      match Llio.int8_from buf with
      | 1 ->
        let nsm_host_id = Llio.string_from buf in
        let namespace_id = Llio.int32_from buf in
        CleanupNsmHostNamespace (nsm_host_id, namespace_id)
      | 2 ->
        let osd_id = Llio.int32_from buf in
        let namespace_id = Llio.int32_from buf in
        CleanupOsdNamespace (osd_id, namespace_id)
      | 3 ->
        let namespace_id = Llio.int32_from buf in
        let osd_id = Llio.int32_from buf in
        CleanupNamespaceOsd (namespace_id, osd_id)
      | 6 ->
        let namespace_id = Llio.int32_from buf in
        let object_id = Llio.string_from buf in
        let object_name = Llio.string_from buf in
        let chunk_id = Llio.int_from buf in
        let fragment_id = Llio.int_from buf in
        let version = Llio.int_from buf in
        RepairBadFragment (namespace_id, object_id, object_name,
                           chunk_id, fragment_id, version)
      | 8 ->
        let osd_id = Llio.int32_from buf in
        let namespace_id = Llio.int32_from buf in
        WaitUntilRepaired (osd_id, namespace_id)
      | 10 ->
        let osd_id = Llio.int32_from buf in
        WaitUntilDecommissioned osd_id
      | 11 ->
        let action = action_from_buffer buf in
        let namespace_id = Llio.int32_from buf in
        let name = Llio.string_from buf in
        let cnt = Llio.int_from buf in
        IterNamespace (action, namespace_id, name, cnt)
      | 12 ->
        let action = action_from_buffer buf in
        let namespace_id = Llio.int32_from buf in
        let name = Llio.string_from buf in
        let range =
          Llio.pair_from
            Llio.string_from
            (Llio.option_from Llio.string_from)
            buf
        in
        IterNamespaceLeaf (action, namespace_id, name, range)
      | 13 ->
        let namespace_id = Llio.int32_from buf in
        let object_id = Llio.string_from buf in
        RewriteObject (namespace_id, object_id)
      | k -> raise_bad_tag "Work" k
  end

  module GetWorkParams = struct
    type t = {
      first : Work.id;
      finc : bool;
      last : (Work.id * bool) option;
      max : int;
      reverse : bool;
    }

    let to_buffer buf { first; finc; last; max; reverse; } =
      let ser_version = 1 in Llio.int8_to buf ser_version;
      x_int64_to buf first;
      Llio.bool_to buf finc;
      Llio.option_to (Llio.pair_to x_int64_to Llio.bool_to) buf last;
      Llio.int_to buf max;
      Llio.bool_to buf reverse

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let first = x_int64_from buf in
      let finc = Llio.bool_from buf in
      let last =
        Llio.option_from
          (Llio.pair_from x_int64_from Llio.bool_from)
          buf in
      let max = Llio.int_from buf in
      let reverse = Llio.bool_from buf in
      { first; finc; last; max; reverse; }
  end

  module Progress = struct
    type base = {
        count : int64;
        next : string option;
      } [@@deriving show]
    let base_to_buffer buf { count; next; } =
      Llio.int64_to buf count;
      Llio.string_option_to buf next
    let base_from_buffer buf =
      let count = Llio.int64_from buf in
      let next = Llio.string_option_from buf in
      { count; next }

    type verify_params = {
        fragments_detected_missing  : int64;
        fragments_osd_unavailable   : int64;
        fragments_checksum_mismatch : int64;
    } [@@deriving show]
    let verify_params_to_buffer
          buf
          { fragments_detected_missing;
            fragments_osd_unavailable;
            fragments_checksum_mismatch; } =
      Llio.int64_to buf fragments_detected_missing;
      Llio.int64_to buf fragments_osd_unavailable;
      Llio.int64_to buf fragments_checksum_mismatch
    let verify_params_from_buffer buf =
      let fragments_detected_missing = Llio.int64_from buf in
      let fragments_osd_unavailable = Llio.int64_from buf in
      let fragments_checksum_mismatch = Llio.int64_from buf in
      { fragments_detected_missing;
        fragments_osd_unavailable;
        fragments_checksum_mismatch }

    type t =
      | Rewrite of base
      | Verify of base * verify_params
    [@@deriving show]

    let to_buffer buf = function
      | Rewrite b ->
        Llio.int8_to buf 1;
        base_to_buffer buf b
      | Verify (b, v) ->
        Llio.int8_to buf 2;
        base_to_buffer buf b;
        verify_params_to_buffer buf v

    let from_buffer buf =
      match Llio.int8_from buf with
      | 1 ->
         let b = base_from_buffer buf in
         Rewrite b
      | 2 ->
         let b = base_from_buffer buf in
         let v = verify_params_from_buffer buf in
         Verify (b, v)
      | k -> raise_bad_tag "Progress" k

    module Update = struct
      type t' = t
      type t =
        | CAS of t' * t' option

      let to_buffer buf = function
        | CAS (old, new_o) ->
          Llio.int8_to buf 1;
          to_buffer buf old;
          Llio.option_to to_buffer buf new_o

      let from_buffer buf =
        match Llio.int8_from buf with
        | 1 ->
          let old = from_buffer buf in
          let new_o = Llio.option_from from_buffer buf in
          CAS (old, new_o)
        | k -> raise_bad_tag "Progress.Update" k
    end
  end

  module RangeQueryArgs = Nsm_protocol.Protocol.RangeQueryArgs
  open Nsm_model
  type ('i, 'o) query =
    | ListNsmHosts : (string RangeQueryArgs.t,
                      (Nsm_host.id * Nsm_host.t * int64) counted_list_more) query
    | ListNamespaces : (string RangeQueryArgs.t,
                        (Namespace.name * Namespace.t) counted_list_more) query

    | ListAvailableOsds : (unit, OsdInfo.t Std.counted_list) query
    | ListOsdsByOsdId : (Osd.id RangeQueryArgs.t,
                         (Osd.id * OsdInfo.t) counted_list_more) query
    | ListOsdsByOsdId2 : (Osd.id RangeQueryArgs.t,
                         (Osd.id * OsdInfo.t) counted_list_more) query
    | ListOsdsByOsdId3 : (Osd.id RangeQueryArgs.t,
                         (Osd.id * OsdInfo.t) counted_list_more) query
    | ListOsdsByLongId : (OsdInfo.long_id RangeQueryArgs.t,
                          (Osd.ClaimInfo.t * OsdInfo.t) counted_list_more) query
    | ListOsdsByLongId2 : (OsdInfo.long_id RangeQueryArgs.t,
                           (Osd.ClaimInfo.t * OsdInfo.t) counted_list_more) query
    | ListOsdsByLongId3 : (OsdInfo.long_id RangeQueryArgs.t,
                           (Osd.ClaimInfo.t * OsdInfo.t) counted_list_more) query

    | ListNamespaceOsds : (Namespace.id * Osd.id RangeQueryArgs.t,
                           (Osd.id * Osd.NamespaceLink.state) counted_list_more) query
    | GetNextMsgs : ('dest, 'msg) Msg_log.t -> ('dest,
                                                (Msg_log.id * 'msg) counted_list_more) query
    | GetWork : (GetWorkParams.t,
                 (Work.id * Work.t) counted_list_more) query
    | GetAlbaId : (unit, alba_id) query
    | ListPresets : (string RangeQueryArgs.t,
                     (Preset.name * Preset.t * bool * bool) counted_list_more) query
    | GetClientConfig : (unit, Alba_arakoon.Config.t) query
    | ListNamespacesById : (Namespace.id RangeQueryArgs.t,
                           (Namespace.id *
                              Namespace.name *
                                Namespace.t) counted_list_more)
                             query
    | GetVersion : (unit, (int * int * int * string)) query
    | CheckClaimOsd : (OsdInfo.long_id, unit) query
    | ListDecommissioningOsds : (Osd.id RangeQueryArgs.t,
                                 (Osd.id * OsdInfo.t) counted_list_more) query
    | ListDecommissioningOsds2 : (Osd.id RangeQueryArgs.t,
                                 (Osd.id * OsdInfo.t) counted_list_more) query
    | ListDecommissioningOsds3 : (Osd.id RangeQueryArgs.t,
                                 (Osd.id * OsdInfo.t) counted_list_more) query
    | ListOsdNamespaces : (Osd.id * Namespace.id RangeQueryArgs.t,
                           Namespace.id counted_list_more) query
    | Statistics : (bool, Generic.t) query
    | CheckLease : (string, int) query
    | GetParticipants : (string, (string * int) counted_list) query
    | GetProgress : (string, Progress.t option) query
    | GetProgressForPrefix : (string, (int * Progress.t) counted_list) query
    | GetMaintenanceConfig : (unit, Maintenance_config.t) query
    | ListPurgingOsds : (Osd.id RangeQueryArgs.t, Osd.id counted_list_more) query

  type ('i, 'o) update =
    | AddNsmHost : (Nsm_host.id * Nsm_host.t, unit) update
    | UpdateNsmHost : (Nsm_host.id * Nsm_host.t, unit) update
    | CreateNamespace : (Namespace.name * Nsm_host.id * Preset.name option, Namespace.t) update
    | DeleteNamespace : (Namespace.name, Nsm_host.id) update
    | RecoverNamespace : (Namespace.name * Nsm_host.id, unit) update
    | AddOsd : (OsdInfo.t, unit) update
    | AddOsd2 : (OsdInfo.t, unit) update
    | AddOsd3 : (OsdInfo.t, unit) update
    | UpdateOsd : (OsdInfo.long_id * Osd.Update.t, unit) update
    | UpdateOsds : ((OsdInfo.long_id * Osd.Update.t) list, unit) update
    | UpdateOsds2 : ((OsdInfo.long_id * Osd.Update.t) list, unit) update
    | DecommissionOsd : (OsdInfo.long_id, unit) update
    | MarkOsdClaimed : (OsdInfo.long_id, Osd.id) update
    | MarkOsdClaimedByOther : (OsdInfo.long_id * alba_id, unit) update
    | MarkMsgDelivered : ('dest, 'msg) Msg_log.t -> ('dest * Msg_log.id, unit) update
    | MarkMsgsDelivered : ('dest, 'msg) Msg_log.t -> ('dest * Msg_log.id, unit) update
    | AddWork : (Work.t Std.counted_list, unit) update
    | MarkWorkCompleted : (Work.id, unit) update
    | CreatePreset : (Preset.name * Preset.t, unit) update
    | DeletePreset : (Preset.name, unit) update
    | SetDefaultPreset : (Preset.name, unit) update
    | AddOsdsToPreset : (Preset.name * Osd.id Std.counted_list, unit) update
    | UpdatePreset : (Preset.name * Preset.Update.t, unit) update
    | StoreClientConfig : (Alba_arakoon.Config.t, unit) update
    | TryGetLease : (string * int, unit) update
    | RegisterParticipant : (string * (string * int), unit) update
    | RemoveParticipant : (string * (string * int), unit) update
    | UpdateProgress : (string * Progress.Update.t, unit) update
    | UpdateMaintenanceConfig : (Maintenance_config.Update.t, Maintenance_config.t) update
    | PurgeOsd : (OsdInfo.long_id, unit) update


  let read_query_i : type i o. (i, o) query -> i Llio.deserializer = function
    | ListNsmHosts -> RangeQueryArgs.from_buffer Llio.string_from
    | ListAvailableOsds -> Llio.unit_from
    | ListOsdsByOsdId   -> RangeQueryArgs.from_buffer Llio.int32_from
    | ListOsdsByOsdId2  -> RangeQueryArgs.from_buffer Llio.int32_from
    | ListOsdsByOsdId3  -> RangeQueryArgs.from_buffer Llio.int32_from
    | ListOsdsByLongId  -> RangeQueryArgs.from_buffer Llio.string_from
    | ListOsdsByLongId2 -> RangeQueryArgs.from_buffer Llio.string_from
    | ListOsdsByLongId3 -> RangeQueryArgs.from_buffer Llio.string_from
    | ListNamespaces -> RangeQueryArgs.from_buffer Llio.string_from
    | ListNamespaceOsds ->
      Llio.pair_from
        Llio.int32_from
        (RangeQueryArgs.from_buffer Llio.int32_from)
    | GetNextMsgs t -> Msg_log.dest_from_buffer t
    | GetWork -> GetWorkParams.from_buffer
    | GetAlbaId -> Llio.unit_from
    | ListPresets -> RangeQueryArgs.from_buffer Llio.string_from
    | GetClientConfig -> Llio.unit_from
    | ListNamespacesById -> RangeQueryArgs.from_buffer Llio.int32_from
    | GetVersion -> Llio.unit_from
    | CheckClaimOsd -> Llio.string_from
    | ListDecommissioningOsds  -> RangeQueryArgs.from_buffer Llio.int32_from
    | ListDecommissioningOsds2 -> RangeQueryArgs.from_buffer Llio.int32_from
    | ListDecommissioningOsds3 -> RangeQueryArgs.from_buffer Llio.int32_from
    | ListOsdNamespaces ->
      Llio.pair_from
        Llio.int32_from
        (RangeQueryArgs.from_buffer Llio.int32_from)
    | Statistics -> Llio.bool_from
    | CheckLease -> Llio.string_from
    | GetParticipants -> Llio.string_from
    | GetProgress -> Llio.string_from
    | GetProgressForPrefix -> Llio.string_from
    | GetMaintenanceConfig -> Llio.unit_from
    | ListPurgingOsds -> RangeQueryArgs.from_buffer Llio.int32_from

  let write_query_i : type i o. (i, o) query -> i Llio.serializer = function
    | ListNsmHosts -> RangeQueryArgs.to_buffer Llio.string_to
    | ListAvailableOsds -> Llio.unit_to
    | ListOsdsByOsdId   -> RangeQueryArgs.to_buffer Llio.int32_to
    | ListOsdsByOsdId2  -> RangeQueryArgs.to_buffer Llio.int32_to
    | ListOsdsByOsdId3  -> RangeQueryArgs.to_buffer Llio.int32_to
    | ListOsdsByLongId  -> RangeQueryArgs.to_buffer Llio.string_to
    | ListOsdsByLongId2 -> RangeQueryArgs.to_buffer Llio.string_to
    | ListOsdsByLongId3 -> RangeQueryArgs.to_buffer Llio.string_to
    | ListNamespaces -> RangeQueryArgs.to_buffer Llio.string_to
    | ListNamespaceOsds ->
      Llio.pair_to
        Llio.int32_to
        (RangeQueryArgs.to_buffer Llio.int32_to)
    | GetNextMsgs t -> Msg_log.dest_to_buffer t
    | GetWork -> GetWorkParams.to_buffer
    | GetAlbaId -> Llio.unit_to
    | ListPresets -> RangeQueryArgs.to_buffer Llio.string_to
    | GetClientConfig -> Llio.unit_to
    | ListNamespacesById -> RangeQueryArgs.to_buffer Llio.int32_to
    | GetVersion -> Llio.unit_to
    | CheckClaimOsd -> Llio.string_to
    | ListDecommissioningOsds  -> RangeQueryArgs.to_buffer Llio.int32_to
    | ListDecommissioningOsds2 -> RangeQueryArgs.to_buffer Llio.int32_to
    | ListDecommissioningOsds3 -> RangeQueryArgs.to_buffer Llio.int32_to
    | ListOsdNamespaces ->
      Llio.pair_to
        Llio.int32_to
        (RangeQueryArgs.to_buffer Llio.int32_to)
    | Statistics -> Llio.bool_to
    | CheckLease -> Llio.string_to
    | GetParticipants -> Llio.string_to
    | GetProgress -> Llio.string_to
    | GetProgressForPrefix -> Llio.string_to
    | GetMaintenanceConfig -> Llio.unit_to
    | ListPurgingOsds -> RangeQueryArgs.to_buffer Llio.int32_to

  let read_query_o : type i o. (i, o) query -> o Llio.deserializer = function
    | ListNsmHosts ->
      counted_list_more_from
        (Llio.tuple3_from
           Llio.string_from
           Nsm_host.from_buffer
           Llio.int64_from)
    | ListAvailableOsds ->
      Llio.counted_list_from OsdInfo.from_buffer
    | ListOsdsByOsdId ->
      counted_list_more_from
        (Llio.pair_from
           Llio.int32_from
           OsdInfo.from_buffer)
    | ListOsdsByOsdId2 ->
      counted_list_more_from
        (Llio.pair_from
           Llio.int32_from
           OsdInfo.from_buffer)
    | ListOsdsByOsdId3 ->
      counted_list_more_from
        (Llio.pair_from
           Llio.int32_from
           OsdInfo.from_buffer)
    | ListOsdsByLongId ->
       counted_list_more_from Osd.from_buffer_with_claim_info
    | ListOsdsByLongId2 ->
       counted_list_more_from Osd.from_buffer_with_claim_info
    | ListOsdsByLongId3 ->
       counted_list_more_from Osd.from_buffer_with_claim_info
    | ListNamespaces ->
      counted_list_more_from
        (Llio.pair_from Llio.string_from Namespace.from_buffer)
    | ListNamespaceOsds ->
      counted_list_more_from
        (Llio.pair_from
           Llio.int32_from
           Osd.NamespaceLink.from_buffer)
    | GetNextMsgs t ->
      counted_list_more_from
        (Llio.pair_from
           Llio.int32_from
           (Msg_log.msg_from_buffer t))
    | GetWork ->
      counted_list_more_from
        (Llio.pair_from x_int64_from Work.from_buffer)
    | GetAlbaId -> Llio.string_from
    | ListPresets ->
      counted_list_more_from
        (Llio.tuple4_from
           Llio.string_from
           Preset.from_buffer
           Llio.bool_from
           Llio.bool_from)
    | GetClientConfig ->
      Alba_arakoon.Config.from_buffer
    | ListNamespacesById ->
       counted_list_more_from
         (Llio.tuple3_from
            Llio.int32_be_from
            Llio.string_from
            Namespace.from_buffer
         )
    | GetVersion ->
       Llio.tuple4_from
         Llio.int_from
         Llio.int_from
         Llio.int_from
         Llio.string_from
    | CheckClaimOsd -> Llio.unit_from
    | ListDecommissioningOsds ->
      counted_list_more_from
        (Llio.pair_from
           Llio.int32_from
           OsdInfo.from_buffer)
    | ListDecommissioningOsds2 ->
       counted_list_more_from
         (Llio.pair_from
            Llio.int32_from
            OsdInfo.from_buffer)
    | ListDecommissioningOsds3 ->
       counted_list_more_from
         (Llio.pair_from
            Llio.int32_from
            OsdInfo.from_buffer)
    | ListOsdNamespaces -> counted_list_more_from Llio.int32_from
    | Statistics     -> Generic.from_buffer
    | CheckLease -> Llio.int_from
    | GetParticipants ->
       Llio.counted_list_from
         (Llio.pair_from
            Llio.string_from
            Llio.int_from)
    | GetProgress -> Llio.option_from Progress.from_buffer
    | GetProgressForPrefix ->
       Llio.counted_list_from
         (Llio.pair_from
            Llio.int_from
            Progress.from_buffer)
    | GetMaintenanceConfig -> Maintenance_config.from_buffer
    | ListPurgingOsds -> counted_list_more_from Llio.int32_from

  let write_query_o : type i o. (i, o) query -> o Llio.serializer = function
    | ListNsmHosts ->
      counted_list_more_to
        (Llio.tuple3_to
           Llio.string_to
           Nsm_host.to_buffer
           Llio.int64_to)
    | ListAvailableOsds ->
      Llio.counted_list_to (OsdInfo.to_buffer ~version:3)
    | ListOsdsByOsdId ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           (OsdInfo.to_buffer ~version:1))
    | ListOsdsByOsdId2 ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           (OsdInfo.to_buffer ~version:2))
    | ListOsdsByOsdId3 ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           (OsdInfo.to_buffer ~version:3))
    | ListOsdsByLongId ->
       counted_list_more_to (Osd.to_buffer_with_claim_info ~version:1)
    | ListOsdsByLongId2 ->
       counted_list_more_to (Osd.to_buffer_with_claim_info ~version:2)
    | ListOsdsByLongId3 ->
       counted_list_more_to (Osd.to_buffer_with_claim_info ~version:3)
    | ListNamespaces ->
      counted_list_more_to
        (Llio.pair_to Llio.string_to Namespace.to_buffer)
    | ListNamespaceOsds ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           Osd.NamespaceLink.to_buffer)
    | GetNextMsgs t ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           (Msg_log.msg_to_buffer t))
    | GetWork ->
      counted_list_more_to
        (Llio.pair_to x_int64_to Work.to_buffer)
    | GetAlbaId -> Llio.string_to
    | ListPresets ->
      counted_list_more_to
        (Llio.tuple4_to
           Llio.string_to
           Preset.to_buffer
           Llio.bool_to
           Llio.bool_to)
    | GetClientConfig ->
      Alba_arakoon.Config.to_buffer
    | ListNamespacesById ->
       counted_list_more_to
         (Llio.tuple3_to
            Llio.int32_be_to
            Llio.string_to
            Namespace.to_buffer
         )
    | GetVersion ->
       Llio.tuple4_to
         Llio.int_to
         Llio.int_to
         Llio.int_to
         Llio.string_to
    | CheckClaimOsd -> Llio.unit_to
    | ListDecommissioningOsds ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           (OsdInfo.to_buffer ~version:1))
    | ListDecommissioningOsds2 ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           (OsdInfo.to_buffer ~version:2))
    | ListDecommissioningOsds3 ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           (OsdInfo.to_buffer ~version:3))
    | ListOsdNamespaces ->
       counted_list_more_to Llio.int32_to
    | Statistics -> Generic.to_buffer
    | CheckLease -> Llio.int_to
    | GetParticipants ->
       Llio.counted_list_to
         (Llio.pair_to
            Llio.string_to
            Llio.int_to)
    | GetProgress -> Llio.option_to Progress.to_buffer
    | GetProgressForPrefix ->
      Llio.counted_list_to
        (Llio.pair_to
           Llio.int_to
           Progress.to_buffer)
    | GetMaintenanceConfig -> Maintenance_config.to_buffer
    | ListPurgingOsds -> counted_list_more_to Llio.int32_to

  let read_update_i : type i o. (i, o) update -> i Llio.deserializer = function
    | AddNsmHost -> Llio.pair_from Llio.string_from Nsm_host.from_buffer
    | UpdateNsmHost -> Llio.pair_from Llio.string_from Nsm_host.from_buffer
    | AddOsd -> OsdInfo.from_buffer
    | AddOsd2 -> OsdInfo.from_buffer
    | AddOsd3 -> OsdInfo.from_buffer
    | UpdateOsd ->
      Llio.pair_from
        Llio.string_from
        Osd.Update.from_buffer
    | UpdateOsds ->
       Llio.list_from
         (Llio.pair_from
            Llio.string_from
            Osd.Update.from_buffer)
    | UpdateOsds2 ->
       Llio.list_from
         (Llio.pair_from
            Llio.string_from
            Osd.Update.from_buffer)
    | DecommissionOsd -> Llio.string_from
    | MarkOsdClaimed -> Llio.string_from
    | MarkOsdClaimedByOther ->
      Llio.pair_from
        Llio.string_from
        Llio.string_from
    | CreateNamespace ->
      Llio.tuple3_from
        Llio.string_from
        Llio.string_from
        (Llio.option_from Llio.string_from)
    | DeleteNamespace -> Llio.string_from
    | RecoverNamespace ->
      Llio.pair_from
        Llio.string_from
        Llio.string_from
    | MarkMsgDelivered t -> Llio.pair_from (Msg_log.dest_from_buffer t) Llio.int32_from
    | MarkMsgsDelivered t -> Llio.pair_from (Msg_log.dest_from_buffer t) Llio.int32_from
    | AddWork -> Llio.counted_list_from Work.from_buffer
    | MarkWorkCompleted -> x_int64_from
    | CreatePreset -> Llio.pair_from Llio.string_from Preset.from_buffer
    | DeletePreset -> Llio.string_from
    | SetDefaultPreset -> Llio.string_from
    | AddOsdsToPreset ->
      Llio.pair_from
        Llio.string_from
        (Llio.counted_list_from Llio.int32_from)
    | UpdatePreset ->
      Llio.pair_from
        Llio.string_from
        Preset.Update.from_buffer
    | StoreClientConfig ->
       Alba_arakoon.Config.from_buffer
    | TryGetLease ->
       Llio.pair_from Llio.string_from Llio.int_from
    | RegisterParticipant ->
      Llio.pair_from
        Llio.string_from
        (Llio.pair_from
           Llio.string_from
           Llio.int_from)
    | RemoveParticipant ->
      Llio.pair_from
        Llio.string_from
        (Llio.pair_from
           Llio.string_from
           Llio.int_from)
    | UpdateProgress -> Llio.pair_from Llio.string_from Progress.Update.from_buffer
    | UpdateMaintenanceConfig -> Maintenance_config.Update.from_buffer
    | PurgeOsd -> Llio.string_from

  let write_update_i : type i o. (i, o) update -> i Llio.serializer = function
    | AddNsmHost -> Llio.pair_to Llio.string_to Nsm_host.to_buffer
    | UpdateNsmHost -> Llio.pair_to Llio.string_to Nsm_host.to_buffer
    | AddOsd -> OsdInfo.to_buffer ~version:1
    | AddOsd2 -> OsdInfo.to_buffer ~version:2
    | AddOsd3 -> OsdInfo.to_buffer ~version:3
    | UpdateOsd -> Llio.pair_to Llio.string_to (Osd.Update.to_buffer ~version:1)
    | UpdateOsds ->
       Llio.list_to (Llio.pair_to Llio.string_to (Osd.Update.to_buffer ~version:1))
    | UpdateOsds2 ->
       Llio.list_to (Llio.pair_to Llio.string_to (Osd.Update.to_buffer ~version:2))
    | DecommissionOsd -> Llio.string_to
    | MarkOsdClaimed -> Llio.string_to
    | MarkOsdClaimedByOther ->
      Llio.pair_to
        Llio.string_to
        Llio.string_to
    | CreateNamespace ->
      Llio.tuple3_to
        Llio.string_to
        Llio.string_to
        (Llio.option_to Llio.string_to)
    | DeleteNamespace -> Llio.string_to
    | RecoverNamespace ->
      Llio.pair_to
        Llio.string_to
        Llio.string_to
    | MarkMsgDelivered t -> Llio.pair_to (Msg_log.dest_to_buffer t) Llio.int32_to
    | MarkMsgsDelivered t -> Llio.pair_to (Msg_log.dest_to_buffer t) Llio.int32_to
    | AddWork -> Llio.counted_list_to Work.to_buffer
    | MarkWorkCompleted -> x_int64_to
    | CreatePreset -> Llio.pair_to Llio.string_to Preset.to_buffer
    | DeletePreset -> Llio.string_to
    | SetDefaultPreset -> Llio.string_to
    | AddOsdsToPreset ->
      Llio.pair_to
        Llio.string_to
        (Llio.counted_list_to Llio.int32_to)
    | UpdatePreset ->
      Llio.pair_to
        Llio.string_to
        Preset.Update.to_buffer
    | StoreClientConfig ->
       Alba_arakoon.Config.to_buffer
    | TryGetLease ->
       Llio.pair_to Llio.string_to Llio.int_to
    | RegisterParticipant ->
      Llio.pair_to
        Llio.string_to
        (Llio.pair_to
           Llio.string_to
           Llio.int_to)
    | RemoveParticipant ->
      Llio.pair_to
        Llio.string_to
        (Llio.pair_to
           Llio.string_to
           Llio.int_to)
    | UpdateProgress -> Llio.pair_to Llio.string_to Progress.Update.to_buffer
    | UpdateMaintenanceConfig -> Maintenance_config.Update.to_buffer
    | PurgeOsd -> Llio.string_to


  let read_update_o : type i o. (i, o) update -> o Llio.deserializer = function
    | AddNsmHost      -> Llio.unit_from
    | UpdateNsmHost   -> Llio.unit_from
    | AddOsd          -> Llio.unit_from
    | AddOsd2         -> Llio.unit_from
    | AddOsd3         -> Llio.unit_from
    | UpdateOsd       -> Llio.unit_from
    | UpdateOsds      -> Llio.unit_from
    | UpdateOsds2      -> Llio.unit_from
    | DecommissionOsd -> Llio.unit_from
    | MarkOsdClaimed  -> Llio.int32_from
    | MarkOsdClaimedByOther -> Llio.unit_from
    | CreateNamespace ->    Namespace.from_buffer
    | DeleteNamespace ->    Llio.string_from
    | RecoverNamespace ->   Llio.unit_from
    | MarkMsgDelivered _ -> Llio.unit_from
    | MarkMsgsDelivered _ ->Llio.unit_from
    | AddWork ->            Llio.unit_from
    | MarkWorkCompleted ->  Llio.unit_from
    | CreatePreset ->       Llio.unit_from
    | DeletePreset ->       Llio.unit_from
    | SetDefaultPreset ->   Llio.unit_from
    | AddOsdsToPreset ->    Llio.unit_from
    | UpdatePreset ->       Llio.unit_from
    | StoreClientConfig ->  Llio.unit_from
    | TryGetLease ->        Llio.unit_from
    | RegisterParticipant ->Llio.unit_from
    | RemoveParticipant ->  Llio.unit_from
    | UpdateProgress     -> Llio.unit_from
    | UpdateMaintenanceConfig -> Maintenance_config.from_buffer
    | PurgeOsd                -> Llio.unit_from

  let write_update_o : type i o. (i, o) update -> o Llio.serializer = function
    | AddNsmHost      -> Llio.unit_to
    | UpdateNsmHost   -> Llio.unit_to
    | AddOsd          -> Llio.unit_to
    | AddOsd2         -> Llio.unit_to
    | AddOsd3         -> Llio.unit_to
    | UpdateOsd       -> Llio.unit_to
    | UpdateOsds      -> Llio.unit_to
    | UpdateOsds2      -> Llio.unit_to
    | DecommissionOsd -> Llio.unit_to
    | MarkOsdClaimed -> Llio.int32_to
    | MarkOsdClaimedByOther -> Llio.unit_to
    | CreateNamespace ->    Namespace.to_buffer
    | DeleteNamespace ->    Llio.string_to
    | RecoverNamespace ->   Llio.unit_to
    | MarkMsgDelivered _ -> Llio.unit_to
    | MarkMsgsDelivered _ ->Llio.unit_to
    | AddWork ->            Llio.unit_to
    | MarkWorkCompleted ->  Llio.unit_to
    | CreatePreset ->       Llio.unit_to
    | DeletePreset ->       Llio.unit_to
    | SetDefaultPreset ->   Llio.unit_to
    | AddOsdsToPreset ->    Llio.unit_to
    | UpdatePreset ->       Llio.unit_to
    | StoreClientConfig ->  Llio.unit_to
    | TryGetLease ->        Llio.unit_to
    | RegisterParticipant ->Llio.unit_to
    | RemoveParticipant ->  Llio.unit_to
    | UpdateProgress     -> Llio.unit_to
    | UpdateMaintenanceConfig -> Maintenance_config.to_buffer
    | PurgeOsd                -> Llio.unit_to


  type request =
    | Wrap_u : _ update -> request
    | Wrap_q : _ query -> request
  let command_map = [ Wrap_q ListNsmHosts,       3l, "ListNsmHosts";
                      Wrap_u AddNsmHost,         4l, "AddNsmHost";
                      Wrap_u UpdateNsmHost,      5l, "UpdateNsmHost";

                      Wrap_q ListNamespaces,     6l, "ListNamespaces";
                      Wrap_u CreateNamespace,    7l, "CreateNamespace";
                      Wrap_u DeleteNamespace,    8l, "DeleteNamespace";

                      Wrap_q ListNamespaceOsds, 11l, "ListNamespaceOsds";
                      Wrap_q (GetNextMsgs Msg_log.Nsm_host), 15l, "GetNextMsgs Msg_log.Nsm_host";
                      Wrap_u (MarkMsgDelivered Msg_log.Nsm_host), 16l, "MarkMsgDelivered Msg_log.Nsm_host";
                      Wrap_q (GetNextMsgs Msg_log.Osd), 17l, "GetNextMsgs Msg_log.Osd";
                      Wrap_u (MarkMsgDelivered Msg_log.Osd), 18l, "MarkMsgDelivered Msg_log.Osd";

                      Wrap_q GetWork, 19l, "GetWork";
                      Wrap_u MarkWorkCompleted, 20l, "MarkWorkCompleted";

                      Wrap_q GetAlbaId, 22l, "GetAlbaId";

                      Wrap_u CreatePreset, 23l, "CreatePreset";
                      Wrap_q ListPresets, 24l, "ListPresets";
                      Wrap_u SetDefaultPreset, 25l, "SetDefaultPreset";
                      Wrap_u DeletePreset, 26l, "DeletePreset";
                      Wrap_u AddOsdsToPreset, 27l, "AddOsdsToPreset";
                      Wrap_u UpdatePreset, 50l, "UpdatePreset";

                      Wrap_u AddOsd, 28l, "AddOsd";
                      Wrap_u MarkOsdClaimed, 29l, "MarkOsdClaimed";
                      Wrap_q ListAvailableOsds, 30l, "ListAvailableOsds";
                      Wrap_q ListOsdsByOsdId, 31l, "ListOsdsByOsdId";
                      Wrap_q ListOsdsByLongId, 32l, "ListOsdsByLongId";
                      Wrap_u MarkOsdClaimedByOther, 33l, "MarkOsdClaimedByOther";
                      Wrap_u UpdateOsd, 34l, "UpdateOsd";

                      Wrap_u AddWork, 35l, "AddWork";

                      Wrap_u StoreClientConfig, 38l, "StoreClientConfig";
                      Wrap_q GetClientConfig, 39l, "GetClientConfig";
                      Wrap_q ListNamespacesById, 41l, "ListNamespacesById";

                      Wrap_u RecoverNamespace, 42l, "RecoverNamespace";

                      Wrap_q GetVersion, 44l, "GetVersion";
                      Wrap_q CheckClaimOsd, 45l, "CheckClaimOsd";
                      Wrap_u DecommissionOsd, 46l, "DecommissionOsd";

                      Wrap_q ListDecommissioningOsds, 47l, "ListDecommissioningOsds";
                      Wrap_q ListOsdNamespaces, 48l, "ListOsdNamespaces";
                      Wrap_u UpdateOsds,        49l, "UpdateOsds";
                      Wrap_q Statistics,        60l, "Statistics";
                      Wrap_q CheckLease, 51l, "CheckLease";
                      Wrap_u TryGetLease, 52l, "TryGetLease";

                      Wrap_q GetParticipants, 53l, "GetParticipants";
                      Wrap_u RegisterParticipant, 54l, "RegisterParticipant";
                      Wrap_u RemoveParticipant, 55l, "RemoveParticipant";

                      Wrap_q GetProgress, 61l, "GetProgress";
                      Wrap_u UpdateProgress, 62l, "UpdateProgress";
                      Wrap_q GetProgressForPrefix, 63l, "GetProgressForPrefix";

                      Wrap_q GetMaintenanceConfig, 64l, "GetMaintenanceConfig";
                      Wrap_u UpdateMaintenanceConfig, 65l, "UpdateMaintenanceConfig";

                      Wrap_u AddOsd2,                  66l, "AddOsd2";
                      Wrap_q ListOsdsByOsdId2,         67l, "ListOsdsByOsdId2";
                      Wrap_q ListOsdsByLongId2,        68l, "ListOsdsByLongId2";
                      Wrap_q ListDecommissioningOsds2, 69l, "ListDecommissioningOsds2";

                      Wrap_u (MarkMsgsDelivered Msg_log.Nsm_host), 71l, "MarkMsgsDelivered Msg_log.Nsm_host";
                      Wrap_u (MarkMsgsDelivered Msg_log.Osd), 72l, "MarkMsgsDelivered Msg_log.Osd";

                      Wrap_u PurgeOsd, 73l, "PurgeOsd";
                      Wrap_q ListPurgingOsds, 74l, "ListPurgingOsds";

                      Wrap_q ListOsdsByOsdId3, 80l, "ListOsdsByOsdId3";
                      Wrap_q ListOsdsByLongId3, 81l, "ListOsdsByLongId3";
                      Wrap_q ListDecommissioningOsds3, 82l, "ListDecommissioningOsds3";
                      Wrap_u AddOsd3, 83l, "AddOsd3";

                      Wrap_u UpdateOsds2, 84l, "UpdateOsds2";
                    ]


  module Error = struct
    type t =
      | Unknown                         [@value 1]

      | Nsm_host_not_lost               [@value 2]
      | Nsm_host_already_exists         [@value 3]
      | Nsm_host_unknown                [@value 4]

      | Namespace_already_exists        [@value 5]
      | Namespace_does_not_exist        [@value 6]

      | Osd_already_exists              [@value 7]
      | Osd_already_decommissioned      [@value 8]
      | Osd_already_linked_to_namespace [@value 9]
      | Osd_unknown                     [@value 10]
      | Osd_already_claimed             [@value 11]
      | Osd_info_mismatch               [@value 12]

      | Preset_does_not_exist           [@value 13]
      | Preset_already_exists           [@value 14]
      | Preset_cant_delete_default      [@value 15]
      | Preset_cant_delete_in_use       [@value 16]
      | Invalid_preset                  [@value 17]

      | Not_master                      [@value 25]
      | Old_plugin_version              [@value 26]
      | Inconsistent_read               [@value 27]
      | Unknown_operation               [@value 28]

      | Claim_lease_mismatch            [@value 29]

      | Progress_does_not_exist         [@value 30]
      | Progress_CAS_failed             [@value 31]
    [@@deriving show, enum]

    exception Albamgr_exn of t * string

    let payload_2s err = function
      | None -> show err
      | Some p -> p

    let failwith ?payload err = raise (Albamgr_exn (err, payload_2s err payload))
    let failwith_lwt ?payload err = Lwt.fail (Albamgr_exn (err, payload_2s err payload))

    let err2int = to_enum
    let int2err x = Option.get_some_default Unknown (of_enum x)
  end

  let wrap_unknown_operation f =
    try f ()
    with Not_found -> Error.(failwith Unknown_operation)

  let tag_to_command =
    let hasht = Hashtbl.create 100 in
    List.iter
      (fun (comm, tag, _) ->
       if Hashtbl.mem hasht tag
       then failwith (Printf.sprintf "%li is used for multiple albamgr commands" tag);
       Hashtbl.add hasht tag comm)
      command_map;
    (fun tag -> wrap_unknown_operation (fun () -> Hashtbl.find hasht tag))

  let tag_to_name =
    let hasht = Hashtbl.create 100 in
    List.iter (fun (_, tag, name) -> Hashtbl.add hasht tag name) command_map;
    (fun tag -> wrap_unknown_operation (fun () -> Hashtbl.find hasht tag))

  let command_to_tag =
    let hasht = Hashtbl.create 100 in
    List.iter (fun (comm, tag, _) -> Hashtbl.add hasht comm tag) command_map;
    (fun comm -> wrap_unknown_operation (fun () -> Hashtbl.find hasht comm))
end
