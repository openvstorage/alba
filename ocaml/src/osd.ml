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

type key = Asd_protocol.key
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
type apply_result =
  | Ok
  | Exn of Error.t

type partial_get_return =
  | Unsupported
  | NotFound
  | Success

class type osd = object

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

  method apply_sequence : priority -> Assert.t list -> Update.t list -> apply_result Lwt.t
  method set_full : bool -> unit Lwt.t
  method get_version : (int * int * int * string) Lwt.t
  method get_long_id : string
  method get_disk_usage : (int64 * int64) Lwt.t
end
