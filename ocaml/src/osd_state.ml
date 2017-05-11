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
  mutable checksum_errors : int64 option;
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
  osd_state.used <- None;
  osd_state.checksum_errors <- None

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
    checksum_errors = None;
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
  t.port <- port

let add_disk_usage t (used, total) =
  t.used <- Some used;
  t.total <- Some total

let add_json t json =
  t.json <- Some json

let add_checksum_errors t = function
  | 0L -> ()
  | count ->
    let c0 =
      match t.checksum_errors with
      | None -> 0L
      | Some c -> c
    in
    t.checksum_errors <- Some (Int64.add c0 count)


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
  let module Llio = Llio2.WriteBuffer in
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
  Llio.option_to Llio.int64_to buf t.total;
  Llio.option_to Llio.int64_to buf t.checksum_errors

let from_buffer buf =
  let module Llio = Llio2.ReadBuffer in
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
  let checksum_errors =
    Llio.maybe_from_buffer
      (Llio.option_from Llio.int64_from) None
      buf
  in
  { disqualified;
    read; write; errors; seen;
    ips; port;
    json;
    total; used;
    checksum_errors;
  }

let deser_state = from_buffer, to_buffer
