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

(* TODO:
   - remove std::exception from llio.cc?
 *)

open Prelude
open Range_query_args2

module ProxyStatistics =
  struct
    include Alba_statistics2
  end

module Protocol = struct

  let magic = 1148837403l
  let version = 1l

  module Amgrp = Albamgr_protocol.Protocol
  module Nsmhp = Nsm_host_protocol.Protocol
  module Nsmp = Nsm_protocol.Protocol

  module Namespace = Amgrp.Namespace

  type object_name = string[@@deriving show]

  type file_name = string

  type encryption_key = string option
  type overwrite = bool
  type may_not_exist = bool

  (* a call with return value has_more may return less than the
     requested amount of objects/namespaces. when all values in
     the range have been delivered has_more=false, otherwise
     has_more=true. *)
  type has_more = bool

  type preset_name = string
  type offset = Int64.t [@@deriving show]
  type length = int [@@deriving show]
  type data = string

  type consistent_read = bool
  type should_cache = bool

  type ('i, 'o) request =
    | ListNamespaces : (string RangeQueryArgs.t,
                        Namespace.name Std.counted_list * has_more) request
    | NamespaceExists : (Namespace.name, bool) request
    | CreateNamespace : (Namespace.name * preset_name option, unit) request
    | DeleteNamespace : (Namespace.name, unit) request

    | ListObjects : (Namespace.name *
                     string RangeQueryArgs.t,
                     object_name Std.counted_list * has_more) request
    | ReadObjectFs : (Namespace.name *
                      object_name *
                      file_name *
                      consistent_read *
                      should_cache,
                      unit) request
    | WriteObjectFs : (Namespace.name *
                       object_name *
                       file_name *
                       overwrite *
                       Checksum.Checksum.t option,
                       unit) request
    | DeleteObject : (Namespace.name *
                      object_name *
                      may_not_exist,
                      unit) request
    | GetObjectInfo : (Namespace.name *
                       object_name *
                       consistent_read *
                       should_cache,
                       Int64.t * Nsm_model.Checksum.t) request
    | ReadObjectsSlices : (Namespace.name *
                             (object_name * (offset * length) list) list *
                           consistent_read,
                           data) request
    | InvalidateCache : (Namespace.name, unit) request
    | DropCache : (Namespace.name, unit) request
    | ProxyStatistics : (bool, ProxyStatistics.t) request
    | GetVersion : (unit, (int * int * int * string)) request
    | OsdView : (unit, (string * Albamgr_protocol.Protocol.Osd.ClaimInfo.t) Std.counted_list
                       * (Albamgr_protocol.Protocol.Osd.id
                          * Nsm_model.OsdInfo.t
                          * Osd_state.t) Std.counted_list) request
    | GetClientConfig : (unit, Alba_arakoon.Config.t) request

  type request' = Wrap : _ request -> request'
  let command_map = [ 1, Wrap ListNamespaces, "ListNamespaces";
                      2, Wrap NamespaceExists, "NamespaceExists";
                      3, Wrap CreateNamespace, "CreateNamespace";
                      4, Wrap DeleteNamespace, "DeleteNamespace";

                      5, Wrap ListObjects, "ListObjects";
                      8, Wrap DeleteObject, "DeleteObject";
                      9, Wrap GetObjectInfo, "GetObjectInfo";
                      10, Wrap ReadObjectFs, "ReadObjectFs";
                      11, Wrap WriteObjectFs, "WriteObjectFs";
                      13, Wrap ReadObjectsSlices, "ReadObjectsSlices";
                      14, Wrap InvalidateCache, "InvalidateCache";
                      15, Wrap ProxyStatistics, "ProxyStatistics";
                      16, Wrap DropCache, "DropCache";
                      17, Wrap GetVersion, "GetVersion";
                      18, Wrap OsdView,    "OsdView";
                      19, Wrap GetClientConfig, "GetClientConfig";
                    ]

  module Error = struct
    type t =
      | Unknown                 [@value 1]
      | OverwriteNotAllowed     [@value 2]
      | ObjectDoesNotExist      [@value 3]
      | NamespaceAlreadyExists  [@value 4]
      | NamespaceDoesNotExist   [@value 5]
      (* | EncryptionKeyRequired   [@value 6] *)
      | ChecksumMismatch        [@value 7]
      | ChecksumAlgoNotAllowed  [@value 8]
      | PresetDoesNotExist      [@value 9]
      | BadSliceLength          [@value 10]
      | OverlappingSlices       [@value 11]
      | SliceOutsideObject      [@value 12]
      | UnknownOperation        [@value 13]
      | FileNotFound            [@value 14]
      | NoSatisfiablePolicy     [@value 15]
      | ProtocolVersionMismatch [@value 17]
    [@@deriving show, enum]

    exception Exn of t

    let failwith err = raise (Exn err)

    let err2int = to_enum
    let int2err x = Option.get_some_default Unknown (of_enum x)
  end

  let wrap_unknown_operation f =
    try f ()
    with Not_found -> Error.(failwith UnknownOperation)

  let command_to_code =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (code, comm, txt) -> Hashtbl.add hasht comm code) command_map;
    (fun comm -> wrap_unknown_operation (fun () -> Hashtbl.find hasht comm))

  let code_to_command =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (code, comm, txt) -> Hashtbl.add hasht code comm) command_map;
    (fun code -> wrap_unknown_operation (fun () -> Hashtbl.find hasht code))

  let code_to_txt =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (code, _, txt) -> Hashtbl.add hasht code txt) command_map;
    (fun code ->
     try Hashtbl.find hasht code with
     | Not_found -> Printf.sprintf "unknown operation %i" code)

  open Llio2
  let deser_request_i : type i o. (i, o) request -> i Deser.t = function
    | ListNamespaces -> RangeQueryArgs.deser' `MaxThenReverse Deser.string
    | NamespaceExists -> Deser.string
    | CreateNamespace -> Deser.tuple2 Deser.string (Deser.option Deser.string)
    | DeleteNamespace -> Deser.string

    | ListObjects ->
      Deser.tuple2
        Deser.string
        (RangeQueryArgs.deser' `MaxThenReverse Deser.string)
    | ReadObjectFs ->
      Deser.tuple5
        Deser.string
        Deser.string
        Deser.string
        Deser.bool
        Deser.bool
    | WriteObjectFs ->
      Deser.tuple5
        Deser.string
        Deser.string
        Deser.string
        Deser.bool
        (Deser.option Checksum_deser.deser')
    | DeleteObject ->
      Deser.tuple3
        Deser.string
        Deser.string
        Deser.bool
    | GetObjectInfo ->
      Deser.tuple4
        Deser.string
        Deser.string
        Deser.bool
        Deser.bool
    | ReadObjectsSlices ->
      Deser.tuple3
        Deser.string
        (Deser.list
           (Deser.pair
              Deser.string
              (Deser.list
                 (Deser.tuple2
                    Deser.int64
                    Deser.int))))
        Deser.bool
    | InvalidateCache -> Deser.string
    | DropCache -> Deser.string
    | ProxyStatistics -> Deser.bool
    | GetVersion      -> Deser.unit
    | OsdView         -> Deser.unit
    | GetClientConfig -> Deser.unit

  let deser_request_o : type i o. (i, o) request -> o Deser.t = function
    | ListNamespaces -> Deser.tuple2 (Deser.counted_list Deser.string) Deser.bool
    | NamespaceExists -> Deser.bool
    | CreateNamespace -> Deser.unit
    | DeleteNamespace -> Deser.unit

    | ListObjects -> Deser.tuple2 (Deser.counted_list Deser.string) Deser.bool
    | ReadObjectFs -> Deser.unit
    | WriteObjectFs -> Deser.unit
    | DeleteObject -> Deser.unit
    | GetObjectInfo -> Deser.tuple2 Deser.int64 Checksum_deser.deser'
    | ReadObjectsSlices -> Deser.string
    | InvalidateCache -> Deser.unit
    | DropCache -> Deser.unit
    | ProxyStatistics -> ProxyStatistics.deser
    | GetVersion -> Deser.tuple4
                      Deser.int
                      Deser.int
                      Deser.int
                      Deser.string
    | OsdView ->
       let deser_claim =
         Deser.counted_list
           (Deser.tuple2 Deser.string Osd_deser.ClaimInfo.deser) in
       let deser_osd   = Osd_deser.OsdInfo.from_buffer,
                         Osd_deser.OsdInfo.to_buffer
       in
       Deser.tuple2
         deser_claim
         (Deser.counted_list
            (Deser.tuple3 Deser.int32 deser_osd Osd_state.deser_state))
    | GetClientConfig ->
       Alba_arakoon_deser.Config.from_buffer, Alba_arakoon_deser.Config.to_buffer
end
