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

module Protocol = struct

  type osd_id = Nsm_model.osd_id [@@deriving show, yojson]

  type namespace_id = int32 [@@deriving show, yojson]
  type namespace_name = string [@@deriving show, yojson]

  module Namespace_message = struct
    type t =
      | LinkOsd of osd_id * Nsm_model.OsdInfo.t
      | UnlinkOsd of osd_id
    [@@deriving show]

    let to_buffer buf = function
      | LinkOsd (osd_id, osd_info) ->
        Llio.int8_to buf 1;
        Llio.int32_to buf osd_id;
        Nsm_model.OsdInfo._to_buffer_1 ~ignore_tls:true buf osd_info
      | UnlinkOsd osd_id ->
        Llio.int8_to buf 2;
        Llio.int32_to buf osd_id

    let from_buffer buf =
      match Llio.int8_from buf with
      | 1 ->
        let osd_id = Llio.int32_from buf in
        let osd_info = Nsm_model.OsdInfo.from_buffer buf in
        LinkOsd (osd_id, osd_info)
      | 2 ->
        let osd_id = Llio.int32_from buf in
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
               Llio.int32_to buf id
             | DeleteNamespace id ->
               Llio.int8_to buf 2;
               Llio.int32_to buf id
             | RecoverNamespace (name, id) ->
               Llio.int8_to buf 3;
               Llio.string_to buf name;
               Llio.int32_to buf id
             | NamespaceMsg (namespace_id, msg) ->
               Llio.int8_to buf 4;
               Llio.int32_to buf namespace_id;
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
             let id = Llio.int32_from buf in
             CreateNamespace (name, id)
           | 2 ->
             let id = Llio.int32_from buf in
             DeleteNamespace id
           | 3 ->
             let name = Llio.string_from buf in
             let id = Llio.int32_from buf in
             RecoverNamespace (name, id)
           | 4 ->
             let id = Llio.int32_from buf in
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
        (* TODO wrap response in error type *)
        ((namespace_id * 'i_) list, 'o_ list) query

  type ('i, 'o) update =
    | CleanupForNamespace : (namespace_id, int) update
    | DeliverMsg : (Message.t * int32, unit) update
    | DeliverMsgs : ((int32 *Message.t) list, unit) update
    | NsmUpdate :
        ('i_, 'o_) Nsm_protocol.Protocol.update ->
        (namespace_id * 'i_, 'o_) update

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
    | CleanupForNamespace -> Llio.int32_from
    | DeliverMsg -> Llio.pair_from Message.from_buffer Llio.int32_from
    | DeliverMsgs -> Llio.list_from
                       (Llio.pair_from
                          Llio.int32_from
                          Message.from_buffer)
    | NsmUpdate u ->
      Llio.pair_from
        Llio.int32_from
        (Nsm_protocol.Protocol.read_update_request u)
  let write_update_i : type i o. (i, o) update -> i serializer = function
    | CleanupForNamespace -> Llio.int32_to
    | DeliverMsg -> Llio.pair_to Message.to_buffer Llio.int32_to
    | DeliverMsgs -> Llio.list_to
                       (Llio.pair_to
                          Llio.int32_to
                          Message.to_buffer)
    | NsmUpdate u ->
      Llio.pair_to
        Llio.int32_to
        (Nsm_protocol.Protocol.write_update_request u)



  let read_update_o : type i o. (i, o) update -> o deserializer = function
    | CleanupForNamespace -> Llio.int_from
    | DeliverMsg -> Llio.unit_from
    | DeliverMsgs -> Llio.unit_from
    | NsmUpdate u -> Nsm_protocol.Protocol.read_update_response u
  let write_update_o : type i o. (i, o) update -> o serializer = function
    | CleanupForNamespace -> Llio.int_to
    | DeliverMsg -> Llio.unit_to
    | DeliverMsgs -> Llio.unit_to
    | NsmUpdate u -> Nsm_protocol.Protocol.write_update_response u

  let read_query_i : type i o. (i, o) query -> i deserializer = function
    | ListNsms -> Llio.unit_from
    | GetVersion -> Llio.unit_from
    | NSMHStatistics -> Llio.bool_from
    | NsmQuery q ->
      Llio.pair_from
        Llio.int32_from
        (Nsm_protocol.Protocol.read_query_request q)
    | NsmsQuery q ->
       Llio.list_from
         (Llio.pair_from
            Llio.int32_from
            (Nsm_protocol.Protocol.read_query_request q))


  let write_query_i : type i o. (i, o) query -> i serializer = function
    | ListNsms -> Llio.unit_to
    | GetVersion -> Llio.unit_to
    | NSMHStatistics -> Llio.bool_to
    | NsmQuery q ->
      Llio.pair_to
        Llio.int32_to
        (Nsm_protocol.Protocol.write_query_request q)
    | NsmsQuery q ->
       Llio.list_to
         (Llio.pair_to
            Llio.int32_to
            (Nsm_protocol.Protocol.write_query_request q))


  let read_query_o : type i o. (i, o) query -> o deserializer = function
    | ListNsms -> Llio.counted_list_from (Llio.pair_from Llio.int32_from namespace_state_from_buf)
    | GetVersion -> Llio.tuple4_from
                      Llio.int_from
                      Llio.int_from
                      Llio.int_from
                      Llio.string_from
    | NSMHStatistics -> NSMHStatistics.from_buffer
    | NsmQuery q -> Nsm_protocol.Protocol.read_query_response q
    | NsmsQuery q -> Llio.list_from (Nsm_protocol.Protocol.read_query_response q)
  let write_query_o : type i o. (i, o) query -> o serializer = function
    | ListNsms -> Llio.counted_list_to (Llio.pair_to Llio.int32_to namespace_state_to_buf)
    | GetVersion -> Llio.tuple4_to
                      Llio.int_to
                      Llio.int_to
                      Llio.int_to
                      Llio.string_to
    | NSMHStatistics -> NSMHStatistics.to_buffer
    | NsmQuery q -> Nsm_protocol.Protocol.write_query_response q
    | NsmsQuery q -> Llio.list_to (Nsm_protocol.Protocol.write_query_response q)
end
