(*
Copyright 2015 Open vStorage NV

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
open Stat
module ProxyStatistics = struct
    include Stat


    module H = struct
        type ('a,'b) t = ('a *'b) list [@@deriving show, yojson]

        let h_to buf a_to b_to t =
          let ab_to buf (a,b) =
            a_to buf a;
            b_to buf b
          in
          Llio.list_to ab_to buf t

        let h_from a_from b_from buf =
          let ab_from buf =
            let a = a_from buf in
            let b = b_from buf in
            (a,b)
          in
          Llio.list_from ab_from buf
        let find t a = List.assoc a t

        let add t a b = (a,b) :: t
      end

    type ns_t = {
        mutable upload: stat;
        mutable download: stat;
        mutable manifest_cached: int;
        mutable manifest_from_nsm  : int;
        mutable manifest_stale : int;
        mutable fragment_cache_hits: int;
        mutable fragment_cache_misses:int;
      }[@@ deriving show, yojson]

    let ns_make () =
      { upload = Stat.make();
        download = Stat.make();
        manifest_cached = 0;
        manifest_from_nsm  = 0;
        manifest_stale = 0;
        fragment_cache_hits = 0;
        fragment_cache_misses = 0;
      }

    let ns_to buf t =
      Stat.stat_to buf t.upload;
      Stat.stat_to buf t.download;
      Llio.int_to buf t.manifest_cached;
      Llio.int_to buf t.manifest_from_nsm;
      Llio.int_to buf t.manifest_stale;
      Llio.int_to buf t.fragment_cache_hits;
      Llio.int_to buf t.fragment_cache_misses

    let ns_from buf =
      let upload   = Stat.stat_from buf in
      let download = Stat.stat_from buf in
      let manifest_cached    = Llio.int_from buf in
      let manifest_from_nsm  = Llio.int_from buf in
      let manifest_stale     = Llio.int_from buf in
      let fragment_cache_hits    = Llio.int_from buf in
      let fragment_cache_misses  = Llio.int_from buf in

      { upload ; download;
        manifest_cached;
        manifest_from_nsm;
        manifest_stale;
        fragment_cache_hits;
        fragment_cache_misses }

    type t = {
        mutable creation:float;
        mutable period: float;
        mutable ns_stats : (string, ns_t) H.t;
      }[@@deriving show, yojson]

    let make () =
      let creation = Unix.gettimeofday () in
      { creation; period = 0.0;
        ns_stats = [];
      }

    let to_buffer buf t =
      let ser_version = 1 in Llio.int8_to buf ser_version;
      Llio.float_to buf t.creation;
      Llio.float_to buf t.period;
      H.h_to buf Llio.string_to ns_to t.ns_stats

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let creation = Llio.float_from buf in
      let period   = Llio.float_from buf in
      let ns_stats = H.h_from Llio.string_from ns_from buf in
      {creation;period;ns_stats}


    let deser = from_buffer, to_buffer

    let stop t = t.period <- Unix.gettimeofday() -. t.creation

    let clone t = { t with creation = t.creation}

    let clear t =
      t.creation <- Unix.gettimeofday ();
      t.period <- 0.0;
      t.ns_stats <- []

    let find t ns =
     try H.find t.ns_stats ns
     with Not_found ->
       let v = ns_make () in
       let () = t.ns_stats <- H.add t.ns_stats ns v in
       v

   let new_upload t ns delta =
     let ns_stats = find t ns in
     ns_stats.upload <- _update ns_stats.upload delta

   let new_download t ns delta manifest_src (fg_hits, fg_misses) =
     let ns_stats = find t ns in
     let () =
       let open Alba_statistics.Statistics in
       match manifest_src with
       | Cache ->
          ns_stats.manifest_cached <- ns_stats.manifest_cached + 1
       | NsmHost ->
          ns_stats.manifest_from_nsm <- ns_stats.manifest_from_nsm + 1
       | Stale ->
          ns_stats.manifest_stale <- ns_stats.manifest_stale + 1
     in
     let () = ns_stats.fragment_cache_hits <-
                ns_stats.fragment_cache_hits  + fg_hits
     in
     let () = ns_stats.fragment_cache_misses <-
                ns_stats.fragment_cache_misses + fg_misses
     in
     ns_stats.download <- _update ns_stats.download delta

end

module Protocol = struct

  let magic = 1148837403l
  let version = 1l

  module Amgrp = Albamgr_protocol.Protocol
  module Nsmhp = Nsm_host_protocol.Protocol
  module Nsmp = Nsm_protocol.Protocol

  module Namespace = Amgrp.Namespace
  module RangeQueryArgs = Nsmp.RangeQueryArgs

  type object_name = string

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
  type offset = Int64.t
  type length = int
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
                      17, Wrap GetVersion, "GetVersion"
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
    (fun code -> wrap_unknown_operation (fun () -> Hashtbl.find hasht code))

  let deser_request_i : type i o. (i, o) request -> i Deser.t = function
    | ListNamespaces -> RangeQueryArgs.deser Deser.string
    | NamespaceExists -> Deser.string
    | CreateNamespace -> Deser.tuple2 Deser.string (Deser.option Deser.string)
    | DeleteNamespace -> Deser.string

    | ListObjects ->
      Deser.tuple2
        Deser.string
        (RangeQueryArgs.deser Deser.string)
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
        (Deser.option Checksum.Checksum.deser)
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
    | GetVersion -> Deser.unit


  let deser_request_o : type i o. (i, o) request -> o Deser.t = function
    | ListNamespaces -> Deser.tuple2 (Deser.counted_list Deser.string) Deser.bool
    | NamespaceExists -> Deser.bool
    | CreateNamespace -> Deser.unit
    | DeleteNamespace -> Deser.unit

    | ListObjects -> Deser.tuple2 (Deser.counted_list Deser.string) Deser.bool
    | ReadObjectFs -> Deser.unit
    | WriteObjectFs -> Deser.unit
    | DeleteObject -> Deser.unit
    | GetObjectInfo -> Deser.tuple2 Deser.int64 Nsm_model.Checksum.deser
    | ReadObjectsSlices -> Deser.string
    | InvalidateCache -> Deser.unit
    | DropCache -> Deser.unit
    | ProxyStatistics -> ProxyStatistics.deser
    | GetVersion -> Deser.tuple4
                      Deser.int
                      Deser.int
                      Deser.int
                      Deser.string
end
