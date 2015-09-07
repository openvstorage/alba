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

module Protocol = struct

  type alba_id = string

  module Arakoon_config = struct
    type cluster_id = string [@@deriving show]
    (* TODO tls/ssl stuff *)
    type node_name = string [@@deriving show]
    type node_client_cfg = { ips : string list;
                             port : int; }
    [@@deriving show]
    type t = cluster_id * (node_name, node_client_cfg) Hashtbl.t

    let show (cluster_id, cfgs) =
      Printf.sprintf "cluster_id = %s , %s"
        cluster_id
        ([%show : (string * node_client_cfg) list]
           (Hashtbl.fold (fun k v acc -> (k,v) :: acc) cfgs []))
    let pp formatter t =
      Format.pp_print_string formatter (show t)


    let to_buffer buf (cluster_id, cfgs) =
      let ser_version = 1 in Llio.int8_to buf ser_version;
      Llio.string_to buf cluster_id;
      Llio.hashtbl_to
        Llio.string_to
        (fun buf ncfg ->
           Llio.list_to Llio.string_to buf ncfg.ips;
           Llio.int_to buf ncfg.port)
        buf
        cfgs

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let cluster_id = Llio.string_from buf in
      let cfgs =
        Llio.hashtbl_from
          (Llio.pair_from
             Llio.string_from
             (fun buf ->
                let ips = Llio.list_from Llio.string_from buf in
                let port = Llio.int_from buf in
                { ips; port; }))
          buf in
      (cluster_id, cfgs)

    let from_config_file fn =
      let inifile = new Inifiles.inifile fn in
      let cluster_id =
        try
          let cids = inifile # getval "global" "cluster_id" in
          Scanf.sscanf cids "%s" (fun s -> s)
        with (Inifiles.Invalid_element _ ) -> failwith "config has no cluster_id" in
      let node_names = Ini.get inifile "global" "cluster" Ini.p_string_list Ini.required in
      let node_cfgs = Hashtbl.create 3 in
      List.iter
        (fun node_name ->
           let ips = Ini.get inifile node_name "ip" Ini.p_string_list Ini.required in
           let get_int x = Ini.get inifile node_name x Ini.p_int Ini.required in
           let port = get_int "client_port" in
           let cfg = { ips; port } in
           Hashtbl.add node_cfgs node_name cfg)
        node_names;
      (cluster_id, node_cfgs)

    let to_arakoon_client_cfg ((cluster_id, cfgs) : t) =
      {
        Arakoon_client_config.cluster_id;
        node_cfgs =
          Hashtbl.fold
            (fun name (cfg : node_client_cfg) acc ->
               (name,
                { Arakoon_client_config.ips = cfg.ips;
                  port = cfg.port }) :: acc)
            cfgs
            [];
        ssl_cfg = None;
      }
  end

  module Nsm_host = struct
    (* in theory this could be anything,
       in practice it will be the arakoon cluster_id *)
    type id = string [@@deriving show, yojson]

    type kind =
      | Arakoon of Arakoon_config.t
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
        Arakoon_config.to_buffer buf cfg

    let from_buffer buf =
      let lost = Llio.bool_from buf in
      let kind = match Llio.int8_from buf with
        | 1 -> Arakoon (Arakoon_config.from_buffer buf)
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
    include Nsm_model.OsdInfo

    type id = Nsm_model.osd_id [@@deriving show, yojson]

    module Update = struct
      type t = {
        ips' : ip list option;
        port' : port option;
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
        ?other' () =
        { ips'; port';
          total'; used';
          seen'; read'; write'; errors';
          other';
        }

      let apply
          osd
          { ips'; port';
            total'; used';
            seen'; read'; write'; errors';
            other';
          }
        =
        let merge ?(compare=compare) l1 l2 =
          let res =
            List.merge
              compare
              l1
              (List.fast_sort compare l2)
          in
          let len = List.length res in
          if len > 10
          then List.drop res (len - 10)
          else res
        in

        let kind =
          let ips, port = get_ips_port osd.kind in
          let ips' = Option.get_some_default ips ips' in
          let port' = Option.get_some_default port port' in
          match osd.kind with
          | Asd (_, _, asd_id) -> Asd (ips', port', asd_id)
          | Kinetic (_, _, k_id) -> Kinetic (ips', port', k_id)
        in

        { node_id = osd.node_id;
          kind; decommissioned = osd.decommissioned;
          other = Option.get_some_default osd.other other';
          total = Option.get_some_default osd.total total';
          used = Option.get_some_default osd.used used';
          seen = merge osd.seen seen';
          read = merge osd.read read';
          write = merge osd.write write';
          errors =
            merge
              ~compare:(fun i1 i2 -> compare (fst i1) (fst i2))
              osd.errors errors';
        }

      let to_buffer buf u =
        let ser_version = 1 in Llio.int8_to buf ser_version;
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

      let from_buffer buf =
        let ser_version = Llio.int8_from buf in
        assert (ser_version = 1);
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
          total'; used';
          seen'; read'; write'; errors';
          other';
        }
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
    end

    let from_buffer_with_claim_info = Llio.pair_from ClaimInfo.from_buffer from_buffer
    let to_buffer_with_claim_info = Llio.pair_to ClaimInfo.to_buffer to_buffer

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

    let get_encryption t encrypt_info =
      let open Nsm_model in
      let open Encryption.Encryption in
      match t.fragment_encryption, encrypt_info with
      | NoEncryption, EncryptInfo.NoEncryption ->
        t.fragment_encryption
      | AlgoWithKey (algo, key), EncryptInfo.Encrypted (algo', id) ->
        if algo = algo'
        then begin
          let id' = EncryptInfo.get_id_for_key key in
          if id = id'
          then t.fragment_encryption
          else failwith "encrypted with another key"
        end else failwith "algo mismatch for decryption"
      | NoEncryption, EncryptInfo.Encrypted _
      | AlgoWithKey _, EncryptInfo.NoEncryption ->
        failwith "encryption & enc_info mismatch during decryption"


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
        policies' [@key "policies"] : (Policy.policy list option [@default None]);
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
    type id = Int32.t
    type t =
      | CleanupNsmHostNamespace of Nsm_host.id * Namespace.id
      | CleanupOsdNamespace of Osd.id * Namespace.id
      | CleanupNamespaceOsd of Namespace.id * Osd.id
      | RepairBadFragment of
          Namespace.id * Nsm_model.object_id * Nsm_model.object_name *
          Nsm_model.chunk_id * Nsm_model.fragment_id * Nsm_model.version
      | WaitUntilRepaired of Osd.id * Namespace.id
      | WaitUntilDecommissioned of Osd.id
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
      | WaitUntilRepaired (osd_id, namespace_id) ->
        Llio.int8_to buf 8;
        Llio.int32_to buf osd_id;
        Llio.int32_to buf namespace_id
      | WaitUntilDecommissioned osd_id ->
        Llio.int8_to buf 10;
        Llio.int32_to buf osd_id

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
      Llio.int32_to buf first;
      Llio.bool_to buf finc;
      Llio.option_to (Llio.pair_to Llio.int32_to Llio.bool_to) buf last;
      Llio.int_to buf max;
      Llio.bool_to buf reverse

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let first = Llio.int32_from buf in
      let finc = Llio.bool_from buf in
      let last =
        Llio.option_from
          (Llio.pair_from Llio.int32_from Llio.bool_from)
          buf in
      let max = Llio.int_from buf in
      let reverse = Llio.bool_from buf in
      { first; finc; last; max; reverse; }
  end

  module RangeQueryArgs = Nsm_protocol.Protocol.RangeQueryArgs

  type ('i, 'o) query =
    | ListNsmHosts : (string RangeQueryArgs.t,
                      (Nsm_host.id * Nsm_host.t * int64) counted_list_more) query
    | ListNamespaces : (string RangeQueryArgs.t,
                        (Namespace.name * Namespace.t) counted_list_more) query

    | ListAvailableOsds : (unit, Osd.t Std.counted_list) query
    | ListOsdsByOsdId : (Osd.id RangeQueryArgs.t,
                         (Osd.id * Osd.t) counted_list_more) query
    | ListOsdsByLongId : (Osd.long_id RangeQueryArgs.t,
                          (Osd.ClaimInfo.t * Osd.t) counted_list_more) query

    | ListNamespaceOsds : (Namespace.id * Osd.id RangeQueryArgs.t,
                           (Osd.id * Osd.NamespaceLink.state) counted_list_more) query
    | GetNextMsgs : ('dest, 'msg) Msg_log.t -> ('dest,
                                                (Msg_log.id * 'msg) counted_list_more) query
    | GetWork : (GetWorkParams.t,
                 (Work.id * Work.t) counted_list_more) query
    | GetAlbaId : (unit, alba_id) query
    | ListPresets : (string RangeQueryArgs.t,
                     (Preset.name * Preset.t * bool * bool) counted_list_more) query
    | GetClientConfig : (unit, Arakoon_config.t) query
    | ListNamespacesById : (Namespace.id RangeQueryArgs.t,
                           (Namespace.id *
                              Namespace.name *
                                Namespace.t) counted_list_more)
                             query
    | GetVersion : (unit, (int * int * int * string)) query
    | CheckClaimOsd : (Osd.long_id, unit) query
    | ListDecommissioningOsds : (Osd.id RangeQueryArgs.t,
                                 (Osd.id * Osd.t) counted_list_more) query
    | ListOsdNamespaces : (Osd.id * Namespace.id RangeQueryArgs.t,
                           Namespace.id counted_list_more) query


  type ('i, 'o) update =
    | AddNsmHost : (Nsm_host.id * Nsm_host.t, unit) update
    | UpdateNsmHost : (Nsm_host.id * Nsm_host.t, unit) update
    | CreateNamespace : (Namespace.name * Nsm_host.id * Preset.name option, Namespace.t) update
    | DeleteNamespace : (Namespace.name, Nsm_host.id) update
    | RecoverNamespace : (Namespace.name * Nsm_host.id, unit) update
    | AddOsd : (Osd.t, unit) update
    | UpdateOsd : (Osd.long_id * Osd.Update.t, unit) update
    | UpdateOsds : ((Osd.long_id * Osd.Update.t) list, unit) update
    | DecommissionOsd : (Osd.long_id, unit) update
    | MarkOsdClaimed : (Osd.long_id, Osd.id) update
    | MarkOsdClaimedByOther : (Osd.long_id * alba_id, unit) update
    | MarkMsgDelivered : ('dest, 'msg) Msg_log.t -> ('dest * Msg_log.id, unit) update
    | AddWork : (Work.t Std.counted_list, unit) update
    | MarkWorkCompleted : (Work.id, unit) update
    | CreatePreset : (Preset.name * Preset.t, unit) update
    | DeletePreset : (Preset.name, unit) update
    | SetDefaultPreset : (Preset.name, unit) update
    | AddOsdsToPreset : (Preset.name * Osd.id Std.counted_list, unit) update
    | UpdatePreset : (Preset.name * Preset.Update.t, unit) update
    | StoreClientConfig : (Arakoon_config.t, unit) update

  let read_query_i : type i o. (i, o) query -> i Llio.deserializer = function
    | ListNsmHosts -> RangeQueryArgs.from_buffer Llio.string_from
    | ListAvailableOsds -> Llio.unit_from
    | ListOsdsByOsdId -> RangeQueryArgs.from_buffer Llio.int32_from
    | ListOsdsByLongId -> RangeQueryArgs.from_buffer Llio.string_from
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
    | ListDecommissioningOsds -> RangeQueryArgs.from_buffer Llio.int32_from
    | ListOsdNamespaces ->
      Llio.pair_from
        Llio.int32_from
        (RangeQueryArgs.from_buffer Llio.int32_from)

  let write_query_i : type i o. (i, o) query -> i Llio.serializer = function
    | ListNsmHosts -> RangeQueryArgs.to_buffer Llio.string_to
    | ListAvailableOsds -> Llio.unit_to
    | ListOsdsByOsdId -> RangeQueryArgs.to_buffer Llio.int32_to
    | ListOsdsByLongId -> RangeQueryArgs.to_buffer Llio.string_to
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
    | ListDecommissioningOsds -> RangeQueryArgs.to_buffer Llio.int32_to
    | ListOsdNamespaces ->
      Llio.pair_to
        Llio.int32_to
        (RangeQueryArgs.to_buffer Llio.int32_to)

  let read_query_o : type i o. (i, o) query -> o Llio.deserializer = function
    | ListNsmHosts ->
      counted_list_more_from
        (Llio.tuple3_from
           Llio.string_from
           Nsm_host.from_buffer
           Llio.int64_from)
    | ListAvailableOsds ->
      Llio.counted_list_from Osd.from_buffer
    | ListOsdsByOsdId ->
      counted_list_more_from
        (Llio.pair_from
           Llio.int32_from
           Osd.from_buffer)
    | ListOsdsByLongId ->
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
        (Llio.pair_from Llio.int32_from Work.from_buffer)
    | GetAlbaId -> Llio.string_from
    | ListPresets ->
      counted_list_more_from
        (Llio.tuple4_from
           Llio.string_from
           Preset.from_buffer
           Llio.bool_from
           Llio.bool_from)
    | GetClientConfig ->
      Arakoon_config.from_buffer
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
           Osd.from_buffer)
    | ListOsdNamespaces ->
      counted_list_more_from Llio.int32_from

  let write_query_o : type i o. (i, o) query -> o Llio.serializer = function
    | ListNsmHosts ->
      counted_list_more_to
        (Llio.tuple3_to
           Llio.string_to
           Nsm_host.to_buffer
           Llio.int64_to)
    | ListAvailableOsds ->
      Llio.counted_list_to Osd.to_buffer
    | ListOsdsByOsdId ->
      counted_list_more_to
        (Llio.pair_to
           Llio.int32_to
           Osd.to_buffer)
    | ListOsdsByLongId ->
      counted_list_more_to Osd.to_buffer_with_claim_info
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
        (Llio.pair_to Llio.int32_to Work.to_buffer)
    | GetAlbaId -> Llio.string_to
    | ListPresets ->
      counted_list_more_to
        (Llio.tuple4_to
           Llio.string_to
           Preset.to_buffer
           Llio.bool_to
           Llio.bool_to)
    | GetClientConfig ->
      Arakoon_config.to_buffer
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
           Osd.to_buffer)
    | ListOsdNamespaces ->
      counted_list_more_to Llio.int32_to

  let read_update_i : type i o. (i, o) update -> i Llio.deserializer = function
    | AddNsmHost -> Llio.pair_from Llio.string_from Nsm_host.from_buffer
    | UpdateNsmHost -> Llio.pair_from Llio.string_from Nsm_host.from_buffer
    | AddOsd -> Osd.from_buffer
    | UpdateOsd ->
      Llio.pair_from
        Llio.string_from
        Osd.Update.from_buffer
    | UpdateOsds ->
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
    | AddWork -> Llio.counted_list_from Work.from_buffer
    | MarkWorkCompleted -> Llio.int32_from
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
      Arakoon_config.from_buffer
  let write_update_i : type i o. (i, o) update -> i Llio.serializer = function
    | AddNsmHost -> Llio.pair_to Llio.string_to Nsm_host.to_buffer
    | UpdateNsmHost -> Llio.pair_to Llio.string_to Nsm_host.to_buffer
    | AddOsd -> Osd.to_buffer
    | UpdateOsd -> Llio.pair_to Llio.string_to Osd.Update.to_buffer
    | UpdateOsds ->
       Llio.list_to (Llio.pair_to Llio.string_to Osd.Update.to_buffer)
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
    | AddWork -> Llio.counted_list_to Work.to_buffer
    | MarkWorkCompleted -> Llio.int32_to
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
      Arakoon_config.to_buffer


  let read_update_o : type i o. (i, o) update -> o Llio.deserializer = function
    | AddNsmHost      -> Llio.unit_from
    | UpdateNsmHost   -> Llio.unit_from
    | AddOsd          -> Llio.unit_from
    | UpdateOsd       -> Llio.unit_from
    | UpdateOsds      -> Llio.unit_from
    | DecommissionOsd -> Llio.unit_from
    | MarkOsdClaimed -> Llio.int32_from
    | MarkOsdClaimedByOther -> Llio.unit_from
    | CreateNamespace -> Namespace.from_buffer
    | DeleteNamespace -> Llio.string_from
    | RecoverNamespace -> Llio.unit_from
    | MarkMsgDelivered _ -> Llio.unit_from
    | AddWork -> Llio.unit_from
    | MarkWorkCompleted -> Llio.unit_from
    | CreatePreset -> Llio.unit_from
    | DeletePreset -> Llio.unit_from
    | SetDefaultPreset -> Llio.unit_from
    | AddOsdsToPreset -> Llio.unit_from
    | UpdatePreset -> Llio.unit_from
    | StoreClientConfig -> Llio.unit_from
  let write_update_o : type i o. (i, o) update -> o Llio.serializer = function
    | AddNsmHost      -> Llio.unit_to
    | UpdateNsmHost   -> Llio.unit_to
    | AddOsd          -> Llio.unit_to
    | UpdateOsd       -> Llio.unit_to
    | UpdateOsds      -> Llio.unit_to
    | DecommissionOsd -> Llio.unit_to
    | MarkOsdClaimed -> Llio.int32_to
    | MarkOsdClaimedByOther -> Llio.unit_to
    | CreateNamespace -> Namespace.to_buffer
    | DeleteNamespace -> Llio.string_to
    | RecoverNamespace -> Llio.unit_to
    | MarkMsgDelivered _ -> Llio.unit_to
    | AddWork -> Llio.unit_to
    | MarkWorkCompleted -> Llio.unit_to
    | CreatePreset -> Llio.unit_to
    | DeletePreset -> Llio.unit_to
    | SetDefaultPreset -> Llio.unit_to
    | AddOsdsToPreset -> Llio.unit_to
    | UpdatePreset -> Llio.unit_to
    | StoreClientConfig -> Llio.unit_to


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
                      Wrap_u UpdateOsds,        49l, "UpdateOsds";                    ]

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
    [@@deriving show, enum]

    exception Albamgr_exn of t * string

    let failwith ?(payload="") err = raise (Albamgr_exn (err, payload))
    let failwith_lwt ?(payload="") err = Lwt.fail (Albamgr_exn (err, payload))

    let err2int = to_enum
    let int2err x = Option.get_some_default Unknown (of_enum x)
  end

  let wrap_unknown_operation f =
    try f ()
    with Not_found -> Error.(failwith Unknown_operation)

  let tag_to_command =
    let hasht = Hashtbl.create 3 in
    List.iter
      (fun (comm, tag, _) ->
       if Hashtbl.mem hasht tag
       then failwith (Printf.sprintf "%li is used for multiple albamgr commands" tag);
       Hashtbl.add hasht tag comm)
      command_map;
    (fun tag -> wrap_unknown_operation (fun () -> Hashtbl.find hasht tag))

  let tag_to_name =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (_, tag, name) -> Hashtbl.add hasht tag name) command_map;
    (fun tag -> wrap_unknown_operation (fun () -> Hashtbl.find hasht tag))

  let command_to_tag =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (comm, tag, _) -> Hashtbl.add hasht comm tag) command_map;
    (fun comm -> wrap_unknown_operation (fun () -> Hashtbl.find hasht comm))
end
