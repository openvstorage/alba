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
open Slice
open Checksum
open Asd_statistics

type key = Slice.t
let show_key = Slice.show_limited_escaped
let pp_key = Slice.pp_limited_escaped

module Value = struct
  type blob =
    | Direct of Slice.t
    | Later of int
  [@@deriving show]
  type t = blob * Checksum.t
  [@@deriving show]

  let blob_to_buffer buf = function
    | Direct value ->
       Llio.int8_to buf 1;
       Slice.to_buffer buf value
    | Later size ->
       Llio.int8_to buf 2;
       Llio.int_to buf size

  let blob_from_buffer buf =
    match Llio.int8_from buf with
    | 1 ->
       let value = Slice.from_buffer buf in
       Direct value
    | 2 ->
       let size = Llio.int_from buf in
       Later size
    | k -> Prelude.raise_bad_tag "Asd_server.Value.blob" k

  let to_buffer buf (blob, cs) =
    blob_to_buffer buf blob;
    Checksum.output buf cs

  let from_buffer buf =
    let blob = blob_from_buffer buf in
    let cs = Checksum.input buf in
    (blob, cs)
end

type value = Slice.t
let show_value = Slice.show_limited_escaped
let pp_value = Slice.pp_limited_escaped

type checksum = Checksum.t [@@deriving show]

let _MAGIC = "aLbA"
let _VERSION = 1l

let incompatible version =
  version <> _VERSION

module Assert = struct
  type t =
    | Value of key * value option
                 [@@deriving show]

  let is_none_assert = function
    | Value (_, None) -> true
    | Value (_, Some _) -> false

  let key_of (Value (k,_)) = k

  let value key value = Value (key, Some value)
  let value_string key value' =
    value
      (Slice.wrap_string key)
      (Slice.wrap_string value')

  let none key = Value (key, None)
  let none_string key = none (Slice.wrap_string key)

  let value_option key vo = Value (key, vo)

  let to_buffer buf = function
    | Value (key, value) ->
      Llio.int8_to buf 1;
      Slice.to_buffer buf key;
      Llio.option_to Slice.to_buffer buf value

  let from_buffer buf =
    match Llio.int8_from buf with
    | 1 ->
      let key = Slice.from_buffer buf in
      let value = Llio.option_from Slice.from_buffer buf in
      Value (key, value)
    | k -> Prelude.raise_bad_tag "Asd_protocol.Assert" k
end

module Update = struct

  type t =
    | Set of key * (value * checksum * bool) option
                                             [@@ deriving show]

  let set k v c b = Set (k, Some (v,c,b))

  let set_string k v c b =
    set (Slice.wrap_string k) (Slice.wrap_string v) c b

  let delete k = Set (k, None)
  let delete_string k = delete (Slice.wrap_string k)

  let to_buffer buf = function
    | Set (key, vcob) ->
      Llio.int8_to buf 1;
      Slice.to_buffer buf key;
      Llio.option_to
        (Llio.tuple3_to
           Slice.to_buffer
           Checksum.output
           Llio.bool_to
        )
        buf
        vcob

  let from_buffer buf =
    match Llio.int8_from buf with
    | 1 ->
      let key = Slice.from_buffer buf in
      let vcob =
        Llio.option_from
          (Llio.tuple3_from
             Slice.from_buffer
             Checksum.input
             Llio.bool_from
          )
          buf
      in
      Set (key, vcob)
    | k -> Prelude.raise_bad_tag "Asd_protocol.Update" k

end

module AsdMgmt = struct
    type t = { latest_disk_usage : (int64 * int64) ref;
               limit: int64;
               mutable full : bool; (* override *)
             }
    let _next_msg_id =
      Slice.wrap_string Osd_keys.AlbaInstance.next_msg_id

    let make latest_disk_usage limit = { latest_disk_usage; limit; full=false }

    let updates_allowed t (updates:Update.t list) =
      let (used, cap) = !(t.latest_disk_usage) in
      Lwt_log.ign_debug_f "updates_allowed?(used:%Li,cap:%Li) full:%b"
                          used cap t.full;
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
      if t.full || (Int64.mul 100L used >= Int64.mul t.limit cap)
      then check_this_update ()
      else true


    let set_full t b = t.full <- b
end

module Protocol = struct
  type range_request = {
    first : Slice.t;
    finc : bool;
    last : (Slice.t * bool) option;
    reverse : bool;
    max : int;
  }
  [@@deriving show]
  let rr_from buf =
    let first = Slice.from_buffer buf in
    let finc = Llio.bool_from buf in
    let last = Llio.option_from (Llio.pair_from Slice.from_buffer Llio.bool_from) buf in
    let reverse = Llio.bool_from buf in
    let max = Llio.int_from buf in
    { first; finc; last; reverse; max }
  let rr_to buf rr =
    Slice.to_buffer buf rr.first;
    Llio.bool_to buf rr.finc;
    Llio.option_to (Llio.pair_to Slice.to_buffer Llio.bool_to) buf rr.last;
    Llio.bool_to buf rr.reverse;
    Llio.int_to buf rr.max

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
      match code with
      | 1 -> Unknown_error (1, Llio.string_from buf)
      | 2 -> Assert_failed (Llio.string_from buf)
      | 4 -> Unknown_operation
      | 6 -> Full
      | 7 -> ProtocolVersionMismatch (Llio.string_from buf)
      | n -> Unknown_error (n, Llio.string_from buf)

    let from_stream code ic =
      let open Lwt.Infix in
      match code with
      | 1 -> Llio.input_string ic >>= fun s ->
             Unknown_error (1, s) |> lwt_fail
      | 2 -> Llio.input_string ic >>= fun s ->
             Assert_failed s |> lwt_fail
      | 4 -> Unknown_operation |> lwt_fail
      | 6 -> Full |> lwt_fail
      | 7 -> Llio.input_string ic >>= fun s ->
             ProtocolVersionMismatch s |> lwt_fail
      | n -> Llio.input_string ic >>= fun s ->
             Unknown_error (n, s) |> lwt_fail

    let deserialize buf =
      let code = Llio.int_from buf in
      deserialize' code buf

    let serialize buf = function
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

  type priority = Asd_io_scheduler.priority =
                | High
                | Low
  let priority_from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> High
    | 2 -> Low
    | k -> raise_bad_tag "Asd_protocol.Priority" k
  let priority_to_buffer buf prio =
    Llio.int8_to
      buf
      (match prio with
       | High -> 1
       | Low -> 2)
  let maybe_priority_from_buffer = maybe_from_buffer priority_from_buffer High

  type ('request, 'response) query =
    | Range : (range_request * priority, key counted_list_more) query
    | MultiGet : (key list * priority, (value * Checksum.t) option list) query
    | RangeEntries : (range_request * priority, (key * value * checksum) counted_list_more)
                       query
    | Statistics: (bool, AsdStatistics.t) query
    | GetVersion: (unit, (int * int * int *string)) query
    | MultiGet2 : (key list * priority, Value.t option list) query
    | MultiExists: (key list * priority, bool list) query
    | GetDiskUsage : (unit, (int64 * int64)) query
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
    (fun code ->
     try Hashtbl.find hasht code with
     | Not_found -> Printf.sprintf "unknown operation %li" code)

  let query_request_serializer : type req res. (req, res) query -> req Llio.serializer
    = function
      | Range -> Llio.pair_to rr_to priority_to_buffer
      | RangeEntries -> Llio.pair_to rr_to priority_to_buffer
      | MultiGet ->
         Llio.pair_to
           (Llio.list_to Slice.to_buffer)
           priority_to_buffer
      | MultiGet2 -> 
         Llio.pair_to
           (Llio.list_to Slice.to_buffer)
           priority_to_buffer
      | Statistics -> Llio.bool_to
      | GetVersion -> Llio.unit_to
      | MultiExists ->
         Llio.pair_to
           (Llio.list_to Slice.to_buffer)
           priority_to_buffer
      | GetDiskUsage -> Llio.unit_to

  let query_request_deserializer : type req res. (req, res) query -> req Llio.deserializer
    = function
    | Range ->
       Llio.pair_from
         rr_from
         maybe_priority_from_buffer
    | RangeEntries ->
       Llio.pair_from
         rr_from
         maybe_priority_from_buffer
    | MultiGet ->
       Llio.pair_from
         (Llio.list_from Slice.from_buffer)
         maybe_priority_from_buffer
    | MultiGet2 ->
       Llio.pair_from
         (Llio.list_from Slice.from_buffer)
         maybe_priority_from_buffer
    | Statistics -> Llio.bool_from
    | GetVersion -> Llio.unit_from
    | MultiExists ->
       Llio.pair_from
         (Llio.list_from Slice.from_buffer)
         maybe_priority_from_buffer
    | GetDiskUsage -> Llio.unit_from

  let query_response_serializer : type req res. (req, res) query -> res Llio.serializer
    = function
      | Range -> counted_list_more_to Slice.to_buffer
      | RangeEntries ->
        counted_list_more_to
          (Llio.tuple3_to
             Slice.to_buffer
             Slice.to_buffer
             Checksum.output)
      | MultiGet ->
         Llio.list_to (Llio.option_to
                         (Llio.pair_to
                            Slice.to_buffer
                            Checksum.output
                         )
                      )
      | MultiGet2 ->
        Llio.list_to (Llio.option_to Value.to_buffer)
      | Statistics -> AsdStatistics.to_buffer
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

  let query_response_deserializer : type req res. (req, res) query -> res Llio.deserializer
    = function
      | Range -> counted_list_more_from Slice.from_buffer
      | RangeEntries ->
        counted_list_more_from
          (Llio.tuple3_from
             Slice.from_buffer
             Slice.from_buffer
             Checksum.input)
      | MultiGet -> Llio.list_from
                      (Llio.option_from
                         (Llio.pair_from
                            Slice.from_buffer
                            Checksum.input
                         )
                      )
      | MultiGet2 ->
        Llio.list_from (Llio.option_from Value.from_buffer)
      | Statistics -> AsdStatistics.from_buffer
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

  let update_request_serializer : type req res. (req, res) update -> req Llio.serializer
    = function
      | Apply -> fun buf (asserts, updates, prio) ->
        Llio.list_to Assert.to_buffer buf asserts;
        Llio.list_to Update.to_buffer buf updates;
        priority_to_buffer buf prio
      | SetFull -> fun buf full ->
        Llio.bool_to buf full

  let update_request_deserializer : type req res. (req, res) update -> req Llio.deserializer
    = function
      | Apply -> fun buf ->
        Lwt_log.ign_debug "Apply deser";
        let asserts = Llio.list_from Assert.from_buffer buf in
        let updates = Llio.list_from Update.from_buffer buf in
        let prio    = maybe_priority_from_buffer buf in
        (asserts, updates, prio)
      | SetFull -> fun buf ->
        Lwt_log.ign_debug "SetFull deser";
        Llio.bool_from buf

  let update_response_serializer : type req res. (req, res) update -> res Llio.serializer
    = function
      | Apply -> Llio.unit_to
      | SetFull -> Llio.unit_to

  let update_response_deserializer : type req res. (req, res) update -> res Llio.deserializer
    = function
      | Apply -> Llio.unit_from
      | SetFull -> Llio.unit_from
end
