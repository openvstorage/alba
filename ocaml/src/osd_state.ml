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

open Prelude

type t = {
  mutable disqualified : bool;
  mutable write : timestamp list;
  mutable read : timestamp list;
  mutable seen : timestamp list;
  mutable errors : (timestamp * string) list;
  mutable ips   : string list option;
  mutable port  : int option;
  mutable json  : string option;
  mutable total : int64 option;
  mutable used  : int64 option;
  }[@@deriving show]

let reset osd_state =
  osd_state.read <- [];
  osd_state.write <- [];
  osd_state.errors <- [];
  osd_state.seen <- [];
  osd_state.ips <- None;
  osd_state.port <- None;
  osd_state.json <- None;
  osd_state.total <- None;
  osd_state.used <- None

let make () = {
    disqualified = false;
    write = [];
    read = [];
    errors = [];
    seen = [];
    ips = None;
    port = None;
    json = None;
    total = None;
    used = None;
  }

let add_error t exn =
  let ts = Unix.gettimeofday () in
  let msg = Printexc.to_string exn in
  let e = ts,msg in
  t.errors <- e :: t.errors

let _head xs = Prelude.List.take 10 xs

let add_write t =
  let ts = Unix.gettimeofday () in
  t.write <- _head (ts :: t.write)

let add_read t =
  let ts = Unix.gettimeofday () in
  t.read <- _head (ts :: t.read)

let add_seen t =
  let ts = Unix.gettimeofday () in
  t.seen <- _head (ts:: t.seen)

let add_ips_port t ips port =
  t.ips <- Some ips;
  t.port <- Some port

let add_disk_usage t (used, total) =
  t.used <- Some used;
  t.total <- Some total

let add_json t json =
  t.json <- Some json


let disqualify t v = t.disqualified <- v

let disqualified t = t.disqualified

let osd_ok t =
  not (disqualified t)
  &&
    (match t.write, t.errors with
     | [], [] -> true
     | [], _ -> false
     | _, [] -> true
     | write_time::_, (error_time, _)::_ -> write_time > error_time)

let to_buffer buf t =
  let ser_version = 1 in
  Llio.int8_to buf ser_version;
  let l = Llio.list_to
  and f = Llio.float_to
  in
  Llio.bool_to buf t.disqualified;
  l f buf t.read;
  l f buf t.write;
  l (Llio.pair_to f Llio.string_to) buf t.errors;
  l f buf t.seen;
  Llio.option_to Llio.string_to buf t.json;
  Llio.option_to (Llio.list_to Llio.string_to) buf t.ips;
  Llio.option_to Llio.int_to buf t.port;
  Llio.option_to Llio.int64_to buf t.used;
  Llio.option_to Llio.int64_to buf t.total

let from_buffer buf =
  let ser_version = Llio.int8_from buf in
  assert (ser_version = 1);
  let l = Llio.list_from
  and f = Llio.float_from in
  let disqualified = Llio.bool_from buf in
  let read = l f buf in
  let write = l f buf in
  let errors = l (Llio.pair_from f Llio.string_from) buf in
  let seen = l f buf in
  let json = Llio.option_from Llio.string_from buf in
  let ips = Llio.option_from (Llio.list_from Llio.string_from) buf in
  let port = Llio.option_from Llio.int_from buf in
  let used = Llio.option_from Llio.int64_from buf in
  let total = Llio.option_from Llio.int64_from buf in
  { disqualified;
    read; write; errors; seen;
    ips; port;
    json;
    total; used;
  }

let deser_state = from_buffer, to_buffer
