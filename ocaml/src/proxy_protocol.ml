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

(* TODO:
   - remove std::exception from llio.cc?
 *)

open Prelude
open Stat
open Range_query_args2

module ProxyStatistics = struct
    include Stat


    module H = struct
        type ('a,'b) t = ('a *'b) list [@@deriving show, yojson]

        let h_to a_to b_to =
          Llio2.WriteBuffer.list_to
            (Llio2.WriteBuffer.pair_to
               a_to b_to)

        let h_from a_from b_from =
          Llio2.ReadBuffer.list_from
            (Llio2.ReadBuffer.pair_from
               a_from b_from)

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

        mutable partial_read_size: stat;
        mutable partial_read_count: stat;
        mutable partial_read_time : stat;
        mutable partial_read_objects: stat;
      }[@@ deriving show, yojson]

    let ns_make () =
      { upload = Stat.make();
        download = Stat.make();
        manifest_cached = 0;
        manifest_from_nsm  = 0;
        manifest_stale = 0;
        fragment_cache_hits = 0;
        fragment_cache_misses = 0;

        partial_read_size    = Stat.make ();
        partial_read_count   = Stat.make ();
        partial_read_time    = Stat.make ();
        partial_read_objects = Stat.make ();
      }

    let ns_to buf t =
      let module Llio = Llio2.WriteBuffer in
      Stat_deser.to_buffer' buf t.upload;
      Stat_deser.to_buffer' buf t.download;
      Llio.int_to buf t.manifest_cached;
      Llio.int_to buf t.manifest_from_nsm;
      Llio.int_to buf t.manifest_stale;
      Llio.int_to buf t.fragment_cache_hits;
      Llio.int_to buf t.fragment_cache_misses;

      Stat_deser.to_buffer' buf t.partial_read_size;
      Stat_deser.to_buffer' buf t.partial_read_count;
      Stat_deser.to_buffer' buf t.partial_read_time;
      Stat_deser.to_buffer' buf t.partial_read_objects

    let ns_from buf =
      let module Llio = Llio2.ReadBuffer in
      let upload   = Stat_deser.from_buffer' buf in
      let download = Stat_deser.from_buffer' buf in
      let manifest_cached    = Llio.int_from buf in
      let manifest_from_nsm  = Llio.int_from buf in
      let manifest_stale     = Llio.int_from buf in
      let fragment_cache_hits    = Llio.int_from buf in
      let fragment_cache_misses  = Llio.int_from buf in
      (* trick to be able to work with <= 0.6.20 proxies *)
      let (partial_read_size,
           partial_read_count,
           partial_read_time,
           partial_read_objects)
        =
        begin
          if Llio.buffer_done buf
          then
            let r = Stat.make () in
            r,r,r,r
          else
            let s = Stat_deser.from_buffer' buf in
            let c = Stat_deser.from_buffer' buf in
            let t = Stat_deser.from_buffer' buf in
            if Llio.buffer_done buf
            then s,c,t,Stat.make()
            else
              let n = Stat_deser.from_buffer' buf in
              s,c,t,n
        end
      in

      { upload ; download;
        manifest_cached;
        manifest_from_nsm;
        manifest_stale;
        fragment_cache_hits;
        fragment_cache_misses;
        partial_read_size;
        partial_read_count;
        partial_read_time;
        partial_read_objects;
      }

    type t = {
        mutable creation:timestamp;
        mutable period: float;
        mutable ns_stats : (string, ns_t) H.t;
      } [@@deriving show, yojson]

    type t' = {
        t : t;
        changed_ns_stats : (string, unit) Hashtbl.t;
      }

    let make () =
      let creation = Unix.gettimeofday () in
      { t = { creation; period = 0.0;
              ns_stats = [];
            };
        changed_ns_stats = Hashtbl.create 3;
      }

    let to_buffer buf t =
      let module Llio = Llio2.WriteBuffer in
      let ser_version = 1 in Llio.int8_to buf ser_version;
      Llio.float_to buf t.creation;
      Llio.float_to buf t.period;
      H.h_to Llio.string_to ns_to buf t.ns_stats

    let from_buffer buf =
      let module Llio = Llio2.ReadBuffer in
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let creation = Llio.float_from buf in
      let period   = Llio.float_from buf in
      let ns_stats = H.h_from Llio.string_from ns_from buf in
      {creation;period;ns_stats}


    let deser = from_buffer, to_buffer

    let stop t = t.period <- Unix.gettimeofday() -. t.creation

    let clone t = { t.t with creation = t.t.creation }

    let clear t =
      Hashtbl.clear t.changed_ns_stats;
      t.t.creation <- Unix.gettimeofday ();
      t.t.period <- 0.0;
      t.t.ns_stats <- []

    let find t ns =
      Hashtbl.replace t.changed_ns_stats ns ();
      try H.find t.t.ns_stats ns
      with Not_found ->
        let v = ns_make () in
        let () = t.t.ns_stats <- H.add t.t.ns_stats ns v in
        v

    let show' ~only_changed t =
      show
        (if only_changed
         then
           { t.t with
             ns_stats =
               List.filter
                 (fun (namespace, _) -> Hashtbl.mem t.changed_ns_stats namespace)
                 t.t.ns_stats
           }
         else t.t)

    let clear_ns_stats_changed t =
      let r = Hashtbl.length t.changed_ns_stats in
      Hashtbl.clear t.changed_ns_stats;
      r

   let new_upload t ns delta =
     let ns_stats = find t ns in
     ns_stats.upload <- _update ns_stats.upload delta

   let incr_manifest_src ns_stats =
     let open Cache in
     function
     | Fast ->
          ns_stats.manifest_cached <- ns_stats.manifest_cached + 1
     | Slow ->
        ns_stats.manifest_from_nsm <- ns_stats.manifest_from_nsm + 1
     | Stale ->
        ns_stats.manifest_stale <- ns_stats.manifest_stale + 1

   let new_download t ns delta manifest_src (fg_hits, fg_misses) =
     let ns_stats = find t ns in
     let () =

       incr_manifest_src ns_stats manifest_src
     in
     let () = ns_stats.fragment_cache_hits <-
                ns_stats.fragment_cache_hits  + fg_hits
     in
     let () = ns_stats.fragment_cache_misses <-
                ns_stats.fragment_cache_misses + fg_misses
     in
     ns_stats.download <- _update ns_stats.download delta

   let new_read_objects_slices
         t ns
         ~total_length ~n_slices ~n_objects ~mf_sources
         ~fc_hits ~fc_misses
         ~took
     =
     let ns_stats = find t ns in
     ns_stats.partial_read_size    <- _update ns_stats.partial_read_size  (float total_length);
     ns_stats.partial_read_count   <- _update ns_stats.partial_read_count (float n_slices);
     ns_stats.partial_read_objects <- _update ns_stats.partial_read_objects (float n_objects);
     ns_stats.partial_read_time    <- _update ns_stats.partial_read_time  took;
     List.iter (incr_manifest_src ns_stats) mf_sources;
     ns_stats.fragment_cache_hits   <- ns_stats.fragment_cache_hits + fc_hits;
     ns_stats.fragment_cache_misses <- ns_stats.fragment_cache_misses + fc_misses;
     ()

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
    | Ping : (float, float) request

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
                      20, Wrap Ping, "Ping";
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
    | Ping            -> Deser.float

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
    | Ping -> Deser.float
end
