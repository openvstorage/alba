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

open! Prelude
open Slice

type key = Asd_protocol.key [@@deriving show]
type value = Lwt_bytes.t
type checksum = Asd_protocol.checksum

type priority = Asd_protocol.Protocol.priority =
              | High
              | Low

module Blob = Asd_protocol.Blob
module Update = Asd_protocol.Update
module Assert = Asd_protocol.Assert

module Osd_namespace_state = struct
  type t =
    | Active

  let from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> Active
    | k -> raise_bad_tag "Osd_namespace_state" k

  let to_buffer buf = function
    | Active -> Llio.int8_to buf 1
end

module Error = Asd_protocol.Protocol.Error

type apply_result' = ((key * string) list) [@@deriving show]

type apply_result = (apply_result', Error.t) Result.t

(*
  | Ok of apply_result'
  | Exn of Error.t [@@ deriving show]
 *)

type partial_get_return =
  | Unsupported
  | NotFound
  | Success

class type key_value_storage =
  object
    method get_exn : priority -> key -> value Lwt.t
    method get_option : priority -> key -> value option Lwt.t

    method multi_get    : priority -> key list -> value option list Lwt.t
    method multi_exists : priority -> key list -> bool list Lwt.t

    method partial_get : priority ->
                         key ->
                         (int * int * Lwt_bytes.t * int) list ->
                         partial_get_return Lwt.t

    method range :
             priority ->
             first:key -> finc:bool ->
             last : (key * bool) option ->
             reverse:bool -> max:int ->
             key counted_list_more Lwt.t

    method range_entries :
             priority ->
             first:key -> finc:bool ->
             last : (key * bool) option ->
             reverse:bool -> max:int ->
             (key * value * checksum) counted_list_more Lwt.t

    method apply_sequence : priority -> Assert.t list -> Update.t list ->
                            apply_result Lwt.t
  end

class type key_value_osd =
  object
    method kvs : key_value_storage

    method set_full : bool -> unit Lwt.t
    method set_slowness : (float * float) option -> unit Lwt.t
    method get_version : Alba_version.t Lwt.t
    method version : unit -> Alba_version.t
    method get_long_id : string
    method get_disk_usage : (int64 * int64) Lwt.t
    method capabilities : Capabilities.OsdCapabilities.t Lwt.t
  end

type namespace_id = int64
type object_id = string
type chunk_id = int
type fragment_id = int
type version_id = int

type full_fragment_id = object_id * chunk_id * fragment_id * version_id

class type osd = object

  method global_kvs : key_value_storage

  method namespace_kvs : namespace_id -> key_value_storage

  (* should be idempotent, don't throw an error when it already exists *)
  method add_namespace : namespace_id -> unit Lwt.t
  (* deletes the namespace, returns None when completely removed *)
  method delete_namespace : namespace_id -> key -> key option Lwt.t

  method set_full : bool -> unit Lwt.t
  method set_slowness : (float * float) option -> unit Lwt.t
  method get_version : Alba_version.t Lwt.t
  method version : unit -> Alba_version.t
  method get_long_id : string

  method get_disk_usage : (int64 * int64) Lwt.t
  method capabilities : Capabilities.OsdCapabilities.t Lwt.t
end

open Lwt.Infix

class osd_wrap_key_value_osd (key_value_osd : key_value_osd) =
let to_global_key namespace_id key =
  Osd_keys.AlbaInstance.to_global_key
    namespace_id
    (key.Slice.buf, key.Slice.offset, key.Slice.length)
  |> Slice.wrap_string
in
object(self :# osd)
  method global_kvs = key_value_osd # kvs

  method namespace_kvs namespace_id =
    let to_global_key = to_global_key namespace_id in
    let last_to_global = function
      | None ->
         Osd_keys.AlbaInstance.to_global_key
           namespace_id
           ("", 0, 0)
         |> Key_value_store.next_prefix
         |> Option.map
              (fun (s, b) ->
               Slice.wrap_string s, b)
      | Some (l, linc) ->
         Some (to_global_key l, linc)
    in
    let from_global_key key =
      let cnt = Osd_keys.AlbaInstance.verify_global_key
                  namespace_id
                  (key.Slice.buf, key.Slice.offset) in
      let open Slice in
      make key.buf (key.offset + cnt) (key.length - cnt)
    in
    object
      method get_option prio key =
        key_value_osd # kvs # get_option
                      prio
                      (to_global_key key)

      method get_exn prio key =
        key_value_osd # kvs # get_exn
                      prio
                      (to_global_key key)

      method multi_exists prio keys =
        key_value_osd # kvs # multi_exists
                      prio
                      (List.map
                         to_global_key
                         keys)

      method multi_get prio keys =
        key_value_osd # kvs # multi_get
                      prio
                      (List.map
                         to_global_key
                         keys)

      method partial_get prio key slices =
        key_value_osd # kvs # partial_get prio (to_global_key key) slices

      method range
               prio
               ~first ~finc ~last
               ~reverse ~max
        =
        key_value_osd # kvs # range
                      prio
                      ~first:(to_global_key first) ~finc
                      ~last:(last_to_global last)
                      ~reverse ~max >>= fun ((cnt, results), has_more) ->
        Lwt.return ((cnt,
                     List.map from_global_key results),
                    has_more)

      method range_entries
               prio
               ~first ~finc ~last
               ~reverse ~max
        =
        key_value_osd # kvs # range_entries
                      prio
                      ~first:(to_global_key first) ~finc
                      ~last:(last_to_global last)
                      ~reverse ~max >>= fun ((cnt, results), has_more) ->
        Lwt.return ((cnt,
                     List.map
                       (fun (key, value, cs) ->
                        from_global_key key,
                        value,
                        cs)
                       results),
                    has_more)

      method apply_sequence prio asserts updates =
        let assert_namespace_active =
          Assert.value_string
            (Osd_keys.AlbaInstance.namespace_status ~namespace_id)
            (Osd_namespace_state.(serialize
                                        to_buffer
                                        Active))
        in
        key_value_osd # kvs # apply_sequence
                      prio
                      (assert_namespace_active
                       :: List.map
                            (function Assert.Value (key, bo) ->
                                      Assert.Value (to_global_key key, bo))
                            asserts)
                      (List.map
                         (function Update.Set (key, x) ->
                                   Update.Set (to_global_key key, x))
                         updates)
        >>= fun r ->
        match r with
        | Ok r' ->
           begin
             let r2 = List.map (fun (gk, v) -> from_global_key gk, v) r' in
             Lwt.return (Ok r2)
           end
        | r -> Lwt.return r


    end

  method add_namespace namespace_id =
    Lwt.return_unit
  method delete_namespace namespace_id first =
    let kvs = self # namespace_kvs namespace_id in
    kvs # range
      Low
      ~first ~finc:true ~last:None
      ~max:200 ~reverse:false >>= fun ((cnt, keys), has_more) ->
    (* using global_kvs here to avoid the effects of 'assert_namespace_active' *)
    self # global_kvs # apply_sequence
        Low
        []
        (List.map
           (fun key -> Update.delete (to_global_key namespace_id key))
           keys) >>= function
    | Ok _ -> Lwt.return (List.last keys)
    | Error e -> Lwt.fail (Asd_protocol.Protocol.Error.Exn e)

  method set_full = key_value_osd # set_full
  method set_slowness = key_value_osd # set_slowness
  method get_version = key_value_osd # get_version
  method version () = key_value_osd # version ()
  method get_long_id = key_value_osd # get_long_id
  method get_disk_usage = key_value_osd # get_disk_usage
  method capabilities = key_value_osd # capabilities
end
