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

module Protocol = struct

  type osd_id = Nsm_model.osd_id [@@deriving show, yojson]

  type namespace_id = int64 [@@deriving show, yojson]
  type namespace_name = string [@@deriving show, yojson]

  module Namespace_message = struct
    type t =
      | LinkOsd of osd_id * Nsm_model.OsdInfo.t
      | UnlinkOsd of osd_id
    [@@deriving show]

    let to_buffer buf = function
      | LinkOsd (osd_id, osd_info) ->
        Llio.int8_to buf 1;
        x_int64_to buf osd_id;
        (* switch for old abms *)
        let to_buffer =
          let open Nsm_model.OsdInfo in
          match osd_info.kind with
          | Asd     (conn_info, _)
          | Kinetic (conn_info, _) ->
             let (_,_,use_tls, use_rdma) = conn_info in
             begin
               match use_tls,use_rdma with
               | false,false -> _to_buffer_1 ~ignore_tls:true
               | true, false -> _to_buffer_2
               | _           -> _to_buffer_3
             end
          | Alba _
          | Alba2 _
          | AlbaProxy _ ->
             _to_buffer_3
        in
        to_buffer buf osd_info
      | UnlinkOsd osd_id ->
        Llio.int8_to buf 2;
        x_int64_to buf osd_id

    let from_buffer buf =
      match Llio.int8_from buf with
      | 1 ->
        let osd_id = x_int64_from buf in
        let osd_info = Nsm_model.OsdInfo.from_buffer buf in
        LinkOsd (osd_id, osd_info)
      | 2 ->
        let osd_id = x_int64_from buf in
        UnlinkOsd osd_id
      | k -> raise_bad_tag "Nsm_host_protocol.Namespace_message" k
  end


  module Message = struct
    type t =
      | CreateNamespace of namespace_name * namespace_id
      | DeleteNamespace of namespace_id
      | RecoverNamespace of namespace_name * namespace_id
      | NamespaceMsg of namespace_id * Namespace_message.t
    [@@deriving show]

    let to_buffer buf msg =
      let s =
        serialize
          (fun buf -> function
             | CreateNamespace (name, id) ->
               Llio.int8_to buf 1;
               Llio.string_to buf name;
               x_int64_to buf id
             | DeleteNamespace id ->
               Llio.int8_to buf 2;
               x_int64_to buf id
             | RecoverNamespace (name, id) ->
               Llio.int8_to buf 3;
               Llio.string_to buf name;
               x_int64_to buf id
             | NamespaceMsg (namespace_id, msg) ->
               Llio.int8_to buf 4;
               x_int64_to buf namespace_id;
               Namespace_message.to_buffer buf msg)
          msg in
      Llio.string_to buf s

    let from_buffer buf =
      let s = Llio.string_from buf in
      deserialize
        (fun buf ->
           match Llio.int8_from buf with
           | 1 ->
             let name = Llio.string_from buf in
             let id = x_int64_from buf in
             CreateNamespace (name, id)
           | 2 ->
             let id = x_int64_from buf in
             DeleteNamespace id
           | 3 ->
             let name = Llio.string_from buf in
             let id = x_int64_from buf in
             RecoverNamespace (name, id)
           | 4 ->
             let id = x_int64_from buf in
             let msg = Namespace_message.from_buffer buf in
             NamespaceMsg (id, msg)
           | k -> raise_bad_tag "Nsm_host_msg" k)
        s
  end

  type namespace_state =
    | Active of namespace_name
    | Recovering of namespace_name

  let namespace_state_to_buf buf = function
    | Active name ->
      Llio.int8_to buf 1;
      Llio.string_to buf name
    | Recovering name ->
      Llio.int8_to buf 2;
      Llio.string_to buf name

  let namespace_state_from_buf buf =
    match Llio.int8_from buf with
    | 1 -> Active (Llio.string_from buf)
    | 2 -> Recovering (Llio.string_from buf)
    | k -> raise_bad_tag "Namespace state" k

  type ('i, 'o) query =
    | ListNsms : (unit, (namespace_id * namespace_state) Std.counted_list) query
    | GetVersion :  (unit, (int * int * int * string)) query
    | NSMHStatistics : (bool, Statistics_collection.Generic.t) query
    | NsmQuery :
        ('i_, 'o_) Nsm_protocol.Protocol.query ->
        (namespace_id * 'i_, 'o_) query
    | NsmsQuery :
        ('i_, 'o_) Nsm_protocol.Protocol.query ->
        ((namespace_id * 'i_) list,
         ('o_, Nsm_model.Err.t) Result.result list) query
    | UpdateSession : ((string * string option) list , (string * string) list) query
    | GetHostVersion: (unit, int32) query

  type ('i, 'o) update =
    | CleanupForNamespace : (namespace_id, int) update
    | DeliverMsg : (Message.t * int64, unit) update
    | DeliverMsgs : ((int64 *Message.t) list, unit) update
    | NsmUpdate :
        ('i_, 'o_) Nsm_protocol.Protocol.update ->
        (namespace_id * 'i_, 'o_) update
    | NsmsUpdate :
        ('i_, 'o_) Nsm_protocol.Protocol.update ->
        ((namespace_id * 'i_) list,
         ('o_, Nsm_model.Err.t) Result.result list) update
    | UpdateHostVersion : ((int32 * int32), int32) update

  type request =
    | Wrap_q : _ query -> request
    | Wrap_u : _ update -> request

  let nsm_query q = Wrap_q (NsmQuery q)
  let nsm_update q = Wrap_u (NsmUpdate q)

  let command_map =
    let open Nsm_protocol.Protocol in

    [ Wrap_q ListNsms, 1l, "ListNsms";
      Wrap_u CleanupForNamespace, 2l, "CleanupForNamespace";
      Wrap_u DeliverMsg, 3l, "DeliverMsg";
      Wrap_u DeliverMsgs, 32l, "DeliverMsgs";
      Wrap_q GetVersion, 4l, "GetVersion";

      nsm_query GetObjectManifestByName, 5l, "GetObjectManifestByName";
      nsm_query GetObjectManifestById, 6l, "GetObjectManifestById";
      nsm_query ListObjects, 7l, "ListObjects";
      nsm_query ListObjectsById, 31l, "ListObjectsById";
      nsm_update PutObject, 8l, "PutObject";
      nsm_update DeleteObject, 9l, "DeleteObject";
      nsm_update UpdateObject, 10l, "UpdateObject";

      nsm_query ListObjectsByOsd, 12l, "ListObjectsByOsd";
      nsm_query ListObjectsByPolicy, 13l, "ListObjectsByPolicy";

      nsm_update MarkKeysDeleted, 14l, "MarkKeysDeleted";
      nsm_query ListDeviceKeysToBeDeleted, 15l, "ListDeviceKeysToBeDeleted";
      nsm_update CleanupOsdKeysToBeDeleted, 16l, "CleanupOsdKeysToBeDeleted";

      nsm_query GetGcEpochs, 18l, "GetGcEpochs";
      nsm_update DisableGcEpoch, 19l, "DisableGcEpoch";
      nsm_update EnableGcEpoch, 20l, "EnableGcEpoch";

      nsm_query GetStats, 21l, "GetStats";

      nsm_query ListActiveOsds, 22l, "ListActiveOsds";

      Wrap_q NSMHStatistics, 30l , "NSMHStatistics";

      Wrap_q (NsmsQuery GetStats), 33l, "Multi GetStats";

      nsm_query MultiExists, 35l, "MultiExists";
      nsm_update ApplySequence, 36l, "ApplySequence";
      nsm_query GetObjectManifestsByName, 37l, "GetObjectManifestsByName";

      nsm_update UpdateObject2, 38l, "UpdateObject2";

      Wrap_u (NsmsUpdate UpdatePreset), 39l, "Multi UpdatePreset";
      Wrap_q UpdateSession, 40l, "UpdateSession";
      nsm_update UpdateObject3, 41l, "UpdateObject3";
      Wrap_q GetHostVersion, 42l , "GetHostVersion";
      Wrap_u UpdateHostVersion, 43l, "UpdateHostVersion";
    ]

  let wrap_unknown_operation f =
    try f ()
    with Not_found -> Nsm_model.Err.(failwith Unknown_operation)

  let tag_to_command =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (comm, tag, _) -> Hashtbl.add hasht tag comm) command_map;
    (fun tag -> wrap_unknown_operation (fun () -> Hashtbl.find hasht tag))

  let tag_to_name =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (_, tag, name) -> Hashtbl.add hasht tag name) command_map;
    (fun tag -> wrap_unknown_operation (fun () -> Hashtbl.find hasht tag))

  let command_to_tag =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (comm, tag, _) -> Hashtbl.add hasht comm tag) command_map;
    (fun comm -> wrap_unknown_operation (fun () -> Hashtbl.find hasht comm))

  type 'a serializer = 'a Llio.serializer
  type 'a deserializer = 'a Llio.deserializer


  module NSMHStatistics = struct
      include Statistics_collection.Generic
      let show t = show_inner t tag_to_name
    end

  let read_update_i : type i o. (i, o) update -> i deserializer = function
    | CleanupForNamespace -> x_int64_from
    | DeliverMsg -> Llio.pair_from Message.from_buffer x_int64_from
    | DeliverMsgs -> Llio.list_from
                       (Llio.pair_from
                          x_int64_from
                          Message.from_buffer)
    | NsmUpdate u ->
      Llio.pair_from
        x_int64_from
        (Nsm_protocol.Protocol.read_update_request u)
    | NsmsUpdate u ->
       Llio.list_from
         (Llio.pair_from
            Llio.int64_from
            (Nsm_protocol.Protocol.read_update_request u))
    | UpdateHostVersion ->
       Llio.pair_from Llio.int32_from Llio.int32_from
  let write_update_i :
  type i o. Nsm_protocol.Session.t ->
       (i, o) update ->
       i serializer
    =
    fun session ->
    function
    | CleanupForNamespace -> x_int64_to
    | DeliverMsg -> Llio.pair_to Message.to_buffer x_int64_to
    | DeliverMsgs -> Llio.list_to
                       (Llio.pair_to
                          x_int64_to
                          Message.to_buffer)
    | NsmUpdate u ->
      Llio.pair_to
        x_int64_to
        (Nsm_protocol.Protocol.write_update_request session u)
    | NsmsUpdate u ->
       Llio.list_to
         (Llio.pair_to
            Llio.int64_to
            (Nsm_protocol.Protocol.write_update_request session u))
    | UpdateHostVersion ->
       Llio.pair_to Llio.int32_to Llio.int32_to



  let read_update_o : type i o. (i, o) update -> o deserializer = function
    | CleanupForNamespace -> Llio.int_from
    | DeliverMsg -> Llio.unit_from
    | DeliverMsgs -> Llio.unit_from
    | NsmUpdate u -> Nsm_protocol.Protocol.read_update_response u
    | NsmsUpdate u -> Llio.list_from
                        (Result.from_buffer
                           (Nsm_protocol.Protocol.read_update_response u)
                           (fun buf -> Nsm_model.Err.int2err (Llio.int8_from buf))
                        )
    | UpdateHostVersion -> Llio.int32_from

  let write_update_o :
  type i o. Nsm_protocol.Session.t ->
       (i, o) update -> o serializer =
    fun session ->
    function
    | CleanupForNamespace -> Llio.int_to
    | DeliverMsg -> Llio.unit_to
    | DeliverMsgs -> Llio.unit_to
    | NsmUpdate u -> Nsm_protocol.Protocol.write_update_response session u
    | NsmsUpdate u -> Llio.list_to
                        (Result.to_buffer
                           (Nsm_protocol.Protocol.write_update_response session u)
                           (fun buf err -> Llio.int8_to buf (Nsm_model.Err.err2int err))
                        )
    | UpdateHostVersion -> Llio.int32_to

  let read_query_i : type i o. (i, o) query -> i deserializer = function
    | ListNsms -> Llio.unit_from
    | GetVersion -> Llio.unit_from
    | NSMHStatistics -> Llio.bool_from
    | NsmQuery q ->
      Llio.pair_from
        x_int64_from
        (Nsm_protocol.Protocol.read_query_request q)
    | NsmsQuery q ->
       Llio.list_from
         (Llio.pair_from
            x_int64_from
            (Nsm_protocol.Protocol.read_query_request q))
    | UpdateSession ->
       Llio.list_from
         (Llio.pair_from
            Llio.string_from
            (Llio.option_from Llio.string_from)
         )
    | GetHostVersion -> Llio.unit_from

  let write_query_i : type i o. (i, o) query -> i serializer = function
    | ListNsms -> Llio.unit_to
    | GetVersion -> Llio.unit_to
    | NSMHStatistics -> Llio.bool_to
    | NsmQuery q ->
      Llio.pair_to
        x_int64_to
        (Nsm_protocol.Protocol.write_query_request q)
    | NsmsQuery q ->
       Llio.list_to
         (Llio.pair_to
            x_int64_to
            (Nsm_protocol.Protocol.write_query_request q))
    | UpdateSession ->
       Llio.list_to
         (Llio.pair_to
            Llio.string_to
            (Llio.option_to Llio.string_to)
         )
    | GetHostVersion -> Llio.unit_to


  let read_query_o : type i o. (i, o) query -> o deserializer = function
    | ListNsms -> Llio.counted_list_from (Llio.pair_from x_int64_from namespace_state_from_buf)
    | GetVersion -> Llio.tuple4_from
                      Llio.int_from
                      Llio.int_from
                      Llio.int_from
                      Llio.string_from
    | NSMHStatistics -> NSMHStatistics.from_buffer
    | NsmQuery q -> Nsm_protocol.Protocol.read_query_response q
    | NsmsQuery q -> Llio.list_from
                       (Result.from_buffer
                          (Nsm_protocol.Protocol.read_query_response q)
                          (fun buf -> Nsm_model.Err.int2err (Llio.int8_from buf))
                       )
    | UpdateSession -> Llio.list_from
                         (Llio.pair_from
                            Llio.string_from
                            Llio.string_from)
    | GetHostVersion -> Llio.int32_from

  let write_query_o : type i o. Nsm_protocol.Session.t -> (i, o) query ->
                           o serializer
    =
    fun session ->
    function
    | ListNsms -> Llio.counted_list_to (Llio.pair_to x_int64_to namespace_state_to_buf)
    | GetVersion -> Llio.tuple4_to
                      Llio.int_to
                      Llio.int_to
                      Llio.int_to
                      Llio.string_to
    | NSMHStatistics -> NSMHStatistics.to_buffer
    | NsmQuery q -> Nsm_protocol.Protocol.write_query_response session q
    | NsmsQuery q -> Llio.list_to
                       (Result.to_buffer
                          (Nsm_protocol.Protocol.write_query_response session q)
                          (fun buf err -> Llio.int8_to buf (Nsm_model.Err.err2int err))
                       )
    | UpdateSession -> Llio.list_to
                         (Llio.pair_to Llio.string_to Llio.string_to)
    | GetHostVersion -> Llio.int32_to
end
