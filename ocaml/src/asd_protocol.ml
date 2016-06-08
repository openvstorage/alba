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
open Slice
open Checksum
open Asd_statistics
open Range_query_args2

(* TODO use a Lwt_bytes.t instead *)
type key = Slice.t
let show_key = Slice.show_limited_escaped
let pp_key = Slice.pp_limited_escaped

module Blob = struct
  type t =
    | Lwt_bytes of Lwt_bytes.t
    | Bigslice of Bigstring_slice.t
    | Bytes of Bytes.t
    | Slice of Slice.t

  let pp formatter t = Format.pp_print_string formatter "<Value>"

  let to_buffer' buf =
    let module L = Llio2.WriteBuffer in
    function
    | Lwt_bytes s -> L.bigstring_slice_to buf (Bigstring_slice.wrap_bigstring s)
    | Bigslice s -> L.bigstring_slice_to buf s
    | Bytes s -> L.string_to buf s
    | Slice s ->
       let open Slice in
       L.substring_to buf (s.buf, s.offset, s.length)

  let from_buffer' buf = Bigslice (Llio2.ReadBuffer.bigstring_slice_from buf)

  let get_slice_unsafe = function
    | Lwt_bytes s -> Slice.wrap_string (Lwt_bytes.to_string s)
    | Bigslice s -> Slice.wrap_string (Bigstring_slice.to_string s)
    | Bytes s -> Slice.wrap_string s
    | Slice s -> s

  let get_string_unsafe x =
    let get () = x |> get_slice_unsafe |> Slice.get_string_unsafe in
    match x with
    | Lwt_bytes _
    | Bigslice _ -> get ()
    | Bytes s -> s
    | Slice s -> Slice.get_string_unsafe s

  let get_bigslice = function
    | Lwt_bytes s -> Bigstring_slice.wrap_bigstring s
    | Bigslice s -> s
    | Bytes s -> Lwt_bytes.of_string s |> Bigstring_slice.wrap_bigstring
    | Slice s -> Slice.to_bigstring s |> Bigstring_slice.wrap_bigstring

  let length = function
    | Lwt_bytes s -> Lwt_bytes.length s
    | Bigslice s -> Bigstring_slice.length s
    | Bytes s -> Bytes.length s
    | Slice s -> Slice.length s

  let write_blob blob fd =
    let len = length blob in
    match blob with
    | Lwt_bytes s ->
       Lwt_extra2.write_all_lwt_bytes
         fd
         s 0 len
    | Bigslice s ->
       let open Bigstring_slice in
       Lwt_extra2.write_all_lwt_bytes
         fd
         s.bs s.offset s.length
    | Bytes s ->
       Lwt_extra2.write_all
         fd
         s 0 len
    | Slice s ->
       let open Slice in
       Lwt_extra2.write_all
         fd
         s.buf s.offset len
end

module Value = struct
  type blob =
    | Direct of Bigstring_slice.t
    | Later of int
  [@@deriving show]
  type t = blob * Checksum.t
  [@@deriving show]

  let blob_to_buffer' buf =
    let module Llio = Llio2.WriteBuffer in
    function
    | Direct value ->
       Llio.int8_to buf 1;
       Llio.bigstring_slice_to buf value
    | Later size ->
       Llio.int8_to buf 2;
       Llio.int_to buf size

  let blob_from_buffer' buf =
    let module Llio = Llio2.ReadBuffer in
    match Llio.int8_from buf with
    | 1 ->
       let value = Llio.bigstring_slice_from buf in
       Direct value
    | 2 ->
       let size = Llio.int_from buf in
       Later size
    | k -> Prelude.raise_bad_tag "Asd_server.Value.blob" k

  let to_buffer' buf (blob, cs) =
    blob_to_buffer' buf blob;
    Checksum_deser.to_buffer' buf cs

  let from_buffer' buf =
    let blob = blob_from_buffer' buf in
    let cs = Checksum_deser.from_buffer' buf in
    (blob, cs)
end

type value = Bigstring_slice.t

type checksum = Checksum.t [@@deriving show]

let _MAGIC = "aLbA"
let _VERSION = 1l

let incompatible version =
  version <> _VERSION

module Assert = struct
  type t =
    | Value of key * Blob.t option
                 [@@deriving show]

  let is_none_assert = function
    | Value (_, None) -> true
    | Value (_, Some _) -> false

  let key_of (Value (k,_)) = k

  let value key value = Value (key, Some value)
  let value_string key value' =
    value
      (Slice.wrap_string key)
      (Blob.Bytes value')

  let none key = Value (key, None)
  let none_string key = none (Slice.wrap_string key)

  let value_option key vo = Value (key, vo)

  let to_buffer' buf =
    let module Llio = Llio2.WriteBuffer in
    function
    | Value (key, value) ->
      Llio.int8_to buf 1;
      Slice.to_buffer' buf key;
      Llio.option_to Blob.to_buffer' buf value

  let from_buffer' buf =
    let module Llio = Llio2.ReadBuffer in
    match Llio.int8_from buf with
    | 1 ->
      let key = Slice.from_buffer' buf in
      let value = Llio.option_from Blob.from_buffer' buf in
      Value (key, value)
    | k -> Prelude.raise_bad_tag "Asd_protocol.Assert" k
end

module Update = struct

  type t =
    | Set of key * (Blob.t * checksum * bool) option
                                              [@@ deriving show]

  let set k v c b = Set (k, Some (v,c,b))

  let set_string k v c b =
    set (Slice.wrap_string k) (Blob.Bytes v) c b

  let delete k = Set (k, None)
  let delete_string k = delete (Slice.wrap_string k)

  let to_buffer' buf =
    let module Llio = Llio2.WriteBuffer in
    function
    | Set (key, vcob) ->
      Llio.int8_to buf 1;
      Slice.to_buffer' buf key;
      Llio.option_to
        (Llio.tuple3_to
           Blob.to_buffer'
           Checksum_deser.to_buffer'
           Llio.bool_to
        )
        buf
        vcob

  let from_buffer' buf =
    let module Llio = Llio2.ReadBuffer in
    match Llio.int8_from buf with
    | 1 ->
      let key = Slice.from_buffer' buf in
      let vcob =
        Llio.option_from
          (Llio.tuple3_from
             Blob.from_buffer'
             Checksum_deser.from_buffer'
             Llio.bool_from
          )
          buf
      in
      Set (key, vcob)
    | k -> Prelude.raise_bad_tag "Asd_protocol.Update" k

end

module AsdMgmt = struct
    type t = { mutable latest_disk_usage : int64;
               capacity : int64 ref;
               limit : int64;
               mutable full : bool; (* override *)
             }
    let _next_msg_id =
      Slice.wrap_string Osd_keys.AlbaInstance.next_msg_id

    let make
          ~latest_disk_usage
          ~capacity
          ~limit
      = { latest_disk_usage; capacity;
          limit;
          full=false }

    let updates_allowed t (updates:Update.t list) =
      let (used, cap) = t.latest_disk_usage, t.capacity in
      Lwt_log.ign_debug_f "updates_allowed?(used:%Li,cap:%Li) full:%b"
                          used !cap t.full;
      let check_this_update () =
        Lwt_log.ign_debug "check_this_update";
        let rec check = function
          | [] -> true
          | Update.Set(_, None) :: updates -> check updates
          | Update.Set(k, Some _) :: _ ->

             let r =
               if Slice.compare k _next_msg_id = 0
               then true
               else false
             in
             Lwt_log.ign_debug_f "check update set for %s:%b"
                                 (Slice.show k) r;
             r
        in
        check updates
      in
      if t.full || (Int64.mul 100L used >= Int64.mul t.limit !cap)
      then check_this_update ()
      else true


    let set_full t b = t.full <- b
end

module Protocol = struct

  module Error = struct
    type t =
      | Unknown_error of int * string
      | Assert_failed of string
      | Unknown_operation
      | Full
      | ProtocolVersionMismatch of string
    [@@deriving show]

    exception Exn of t

    let failwith t = raise (Exn t)
    let lwt_fail t = Lwt.fail (Exn t)

    let get_code = function
      | Unknown_error _ -> 1
      | Assert_failed _ -> 2
      | Unknown_operation -> 4
      | Full  -> 6
      | ProtocolVersionMismatch _ -> 7

    let deserialize' code buf =
      let module Llio = Llio2.ReadBuffer in
      match code with
      | 1 -> Unknown_error (1, Llio.string_from buf)
      | 2 -> Assert_failed (Llio.string_from buf)
      | 4 -> Unknown_operation
      | 6 -> Full
      | 7 -> ProtocolVersionMismatch (Llio.string_from buf)
      | n -> Unknown_error (n, Llio.string_from buf)

    let from_stream code ic =
      let open Lwt.Infix in
      let module Llio = Llio2.NetFdReader in
      match code with
      | 1 -> Llio.string_from ic >>= fun s ->
             Unknown_error (1, s) |> lwt_fail
      | 2 -> Llio.string_from ic >>= fun s ->
             Assert_failed s |> lwt_fail
      | 4 -> Unknown_operation |> lwt_fail
      | 6 -> Full |> lwt_fail
      | 7 -> Llio.string_from ic >>= fun s ->
             ProtocolVersionMismatch s |> lwt_fail
      | n -> Llio.string_from ic >>= fun s ->
             Unknown_error (n, s) |> lwt_fail

    let deserialize buf =
      let module Llio = Llio2.ReadBuffer in
      let code = Llio.int_from buf in
      deserialize' code buf

    let serialize buf =
      let module Llio = Llio2.WriteBuffer in
      function
      | Unknown_error (c, msg) ->
        Llio.int_to buf c;
        Llio.string_to buf msg
      | Assert_failed msg ->
        Llio.int_to buf 2;
        Llio.string_to buf msg
      | Unknown_operation ->
        Llio.int_to buf 4
      | Full ->
        Llio.int_to buf 6
      | ProtocolVersionMismatch msg ->
         Llio.int_to buf 7;
         Llio.string_to buf msg
  end

  type target = {
    hosts : string list;
    port : int;
    asd_id : string;
  }
  let target_from buf =
    let hosts = Llio.list_from Llio.string_from buf in
    let port = Llio.int_from buf in
    let asd_id = Llio.string_from buf in
    { hosts; port; asd_id }

  let target_to buf { hosts; port ; asd_id } =
    Llio.list_to Llio.string_to buf hosts;
    Llio.int_to buf port;
    Llio.string_to buf asd_id

  type has_more = bool

  type priority =
    | High
    | Low
  let priority_from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> High
    | 2 -> Low
    | k -> raise_bad_tag "Asd_protocol.Priority" k
  let priority_from_buffer' buf =
    match Llio2.ReadBuffer.int8_from buf with
    | 1 -> High
    | 2 -> Low
    | k -> raise_bad_tag "Asd_protocol.Priority" k
  let priority_to_buffer buf prio =
    Llio.int8_to
      buf
      (match prio with
       | High -> 1
       | Low -> 2)
  let priority_to_buffer' buf prio =
    Llio2.WriteBuffer.int8_to
      buf
      (match prio with
       | High -> 1
       | Low -> 2)
  let maybe_priority_from_buffer = maybe_from_buffer priority_from_buffer High
  let maybe_priority_from_buffer' =
    Llio2.ReadBuffer.maybe_from_buffer
      priority_from_buffer'
      High

  type ('request, 'response) query =
    | Range : (Slice.t RangeQueryArgs.t * priority, key counted_list_more) query
    | MultiGet : (key list * priority, (Bigstring_slice.t * Checksum.t) option list) query
    | RangeEntries : (Slice.t RangeQueryArgs.t * priority,
                      (key * Bigstring_slice.t * checksum) counted_list_more)
                       query
    | Statistics: (bool, AsdStatistics.t) query
    | GetVersion: (unit, (int * int * int *string)) query
    | MultiGet2 : (key list * priority, Value.t option list) query
    | MultiExists: (key list * priority, bool list) query
    | GetDiskUsage : (unit, (int64 * int64)) query
    | PartialGet : (key * (int * int) list * priority, bool) query
  [@deriving show]

  type ('request, 'response) update =
    | Apply : (Assert.t list * Update.t list * priority, unit) update
    | SetFull: (bool, unit) update

  type t =
    | Wrap_query : _ query -> t
    | Wrap_update : _ update -> t

  let command_map = [ Wrap_query Range,        1l, "Range";
                      Wrap_query MultiGet,     2l, "MultiGet";
                      Wrap_update Apply,       3l, "Apply";
                      Wrap_query RangeEntries, 4l, "RangeEntries";
                      Wrap_query Statistics,   5l, "Statistics";
                      Wrap_update SetFull,     6l, "SetFull";
                      Wrap_query GetVersion,   7l, "GetVersion";
                      Wrap_query MultiGet2,    8l, "MultiGet2";
                      Wrap_query MultiExists,  9l, "MultiExists";
                      Wrap_query GetDiskUsage, 10l, "GetDiskUsage";
                      Wrap_query PartialGet,   11l, "PartialGet";
                      (* the range from 40l to 80l
                         is (currently) taken by internal statistics.
                         see blob_access.ml
                       *)
                    ]

  let wrap_unknown_operation f =
    try f ()
    with Not_found -> Error.(failwith Unknown_operation)

  let command_to_code =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (comm, code, _) -> Hashtbl.add hasht comm code) command_map;
    (fun comm -> wrap_unknown_operation (fun () -> Hashtbl.find hasht comm))

  let code_to_command =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (comm, code, _) -> Hashtbl.add hasht code comm) command_map;
    (fun code -> wrap_unknown_operation (fun () -> Hashtbl.find hasht code))

  let code_to_description =
    let hasht = Hashtbl.create 3 in
    List.iter (fun (_, code, desc) -> Hashtbl.add hasht code desc) command_map;
    (fun code -> Hashtbl.find hasht code)

  let code_to_description_nothrow x =
    try code_to_description x
    with | Not_found -> Printf.sprintf "unknown operation %li" x


  let query_request_serializer : type req res. (req, res) query -> req Llio2.serializer
    =
    let module Llio = Llio2.WriteBuffer in
    function
      | Range ->
         Llio.pair_to
           (RangeQueryArgs.to_buffer' `ReverseThenMax Slice.to_buffer')
           priority_to_buffer'
      | RangeEntries ->
         Llio.pair_to
           (RangeQueryArgs.to_buffer' `ReverseThenMax Slice.to_buffer')
           priority_to_buffer'
      | MultiGet ->
         Llio.pair_to
           (Llio.list_to Slice.to_buffer')
           priority_to_buffer'
      | MultiGet2 ->
         Llio.pair_to
           (Llio.list_to Slice.to_buffer')
           priority_to_buffer'
      | Statistics -> Llio.bool_to
      | GetVersion -> Llio.unit_to
      | MultiExists ->
         Llio.pair_to
           (Llio.list_to Slice.to_buffer')
           priority_to_buffer'
      | GetDiskUsage -> Llio.unit_to
      | PartialGet ->
         Llio.tuple3_to
           Slice.to_buffer'
           (Llio.list_to
              (Llio.pair_to
                 Llio.int_to
                 Llio.int_to))
           priority_to_buffer'

  let query_request_deserializer : type req res. (req, res) query -> req Llio2.deserializer
    =
    let module Llio = Llio2.ReadBuffer in
    function
    | Range ->
       Llio.pair_from
         (RangeQueryArgs.from_buffer' `ReverseThenMax Slice.from_buffer')
         maybe_priority_from_buffer'
    | RangeEntries ->
       Llio.pair_from
         (RangeQueryArgs.from_buffer' `ReverseThenMax Slice.from_buffer')
         maybe_priority_from_buffer'
    | MultiGet ->
       Llio.pair_from
         (Llio.list_from Slice.from_buffer')
         maybe_priority_from_buffer'
    | MultiGet2 ->
       Llio.pair_from
         (Llio.list_from Slice.from_buffer')
         maybe_priority_from_buffer'
    | Statistics -> Llio.bool_from
    | GetVersion -> Llio.unit_from
    | MultiExists ->
       Llio.pair_from
         (Llio.list_from Slice.from_buffer')
         maybe_priority_from_buffer'
    | GetDiskUsage -> Llio.unit_from
    | PartialGet ->
       Llio.tuple3_from
         Slice.from_buffer'
         (Llio.list_from
            (Llio.pair_from
               Llio.int_from
               Llio.int_from))
         priority_from_buffer'

  let query_response_serializer : type req res. (req, res) query -> res Llio2.serializer
    =
    let module Llio = Llio2.WriteBuffer in
    function
      | Range -> Llio.counted_list_more_to Slice.to_buffer'
      | RangeEntries ->
        Llio.counted_list_more_to
          (Llio.tuple3_to
             Slice.to_buffer'
             Llio.bigstring_slice_to
             Checksum_deser.to_buffer')
      | MultiGet ->
         Llio.list_to (Llio.option_to
                         (Llio.pair_to
                            Llio.bigstring_slice_to
                            Checksum_deser.to_buffer'
                         )
                      )
      | MultiGet2 ->
        Llio.list_to (Llio.option_to Value.to_buffer')
      | Statistics -> AsdStatistics.to_buffer'
      | GetVersion ->
         (Llio.tuple4_to
           Llio.int_to
           Llio.int_to
           Llio.int_to
           Llio.string_to)
      | MultiExists -> Llio.list_to Llio.bool_to
      | GetDiskUsage ->
         Llio.pair_to
           Llio.int64_to
           Llio.int64_to
      | PartialGet -> Llio.bool_to

  let query_response_deserializer : type req res. (req, res) query -> res Llio2.deserializer
    =
    let module Llio = Llio2.ReadBuffer in
    function
      | Range -> Llio.counted_list_more_from Slice.from_buffer'
      | RangeEntries ->
        Llio.counted_list_more_from
          (Llio.tuple3_from
             Slice.from_buffer'
             Llio.bigstring_slice_from
             Checksum_deser.from_buffer')
      | MultiGet -> Llio.list_from
                      (Llio.option_from
                         (Llio.pair_from
                            Llio.bigstring_slice_from
                            Checksum_deser.from_buffer'
                         )
                      )
      | MultiGet2 ->
        Llio.list_from (Llio.option_from Value.from_buffer')
      | Statistics -> AsdStatistics.from_buffer'
      | GetVersion -> (Llio.tuple4_from
                         Llio.int_from
                         Llio.int_from
                         Llio.int_from
                         Llio.string_from
                      )
      | MultiExists -> Llio.list_from Llio.bool_from
      | GetDiskUsage ->
         Llio.pair_from
           Llio.int64_from
           Llio.int64_from
      | PartialGet -> Llio.bool_from

  let update_request_serializer : type req res. (req, res) update -> req Llio2.serializer
    =
    let module Llio = Llio2.WriteBuffer in
    function
      | Apply -> fun buf (asserts, updates, prio) ->
        Llio.list_to Assert.to_buffer' buf asserts;
        Llio.list_to Update.to_buffer' buf updates;
        priority_to_buffer' buf prio
      | SetFull -> fun buf full ->
        Llio.bool_to buf full

  let update_request_deserializer : type req res. (req, res) update -> req Llio2.deserializer
    =
    let module Llio = Llio2.ReadBuffer in
    function
      | Apply -> fun buf ->
        Lwt_log.ign_debug "Apply deser";
        let asserts = Llio.list_from Assert.from_buffer' buf in
        let updates = Llio.list_from Update.from_buffer' buf in
        let prio    = maybe_priority_from_buffer' buf in
        (asserts, updates, prio)
      | SetFull -> fun buf ->
        Lwt_log.ign_debug "SetFull deser";
        Llio.bool_from buf

  let update_response_serializer : type req res. (req, res) update -> res Llio2.serializer
    =
    let module Llio = Llio2.WriteBuffer in
    function
      | Apply -> Llio.unit_to
      | SetFull -> Llio.unit_to

  let update_response_deserializer : type req res. (req, res) update -> res Llio2.deserializer
    =
    let module Llio = Llio2.ReadBuffer in
    function
      | Apply -> Llio.unit_from
      | SetFull -> Llio.unit_from
end
