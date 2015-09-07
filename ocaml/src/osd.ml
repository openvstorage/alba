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

type key = Asd_protocol.key
type value = Asd_protocol.value
type checksum = Asd_protocol.checksum

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

class type osd = object

  method get_exn : key -> value Lwt.t
  method get_option : key -> value option Lwt.t

  method multi_get : key list -> value option list Lwt.t

  method range :
    first:key -> finc:bool ->
    last : (key * bool) option ->
    reverse:bool -> max:int ->
    key counted_list_more Lwt.t

  method range_entries :
    first:key -> finc:bool ->
    last : (key * bool) option ->
    reverse:bool -> max:int ->
    (key * value * checksum) counted_list_more Lwt.t

  method apply_sequence : Assert.t list -> Update.t list -> apply_result Lwt.t
  method set_full : bool -> unit Lwt.t
  method get_version : (int * int * int * string) Lwt.t
  method get_long_id : string
end
