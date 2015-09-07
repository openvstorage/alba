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

open Asd_protocol
open Lwt
open Generic_bench


let deletes (client: Osd.osd) n value_size period prefix =
  let gen = make_key period prefix in
  let do_one i =
    let key = gen () in
    let open Slice in
    let key_slice = Slice.wrap_string key in
    let delete = Update.Set (key_slice, None) in
    let updates = [delete] in
    client # apply_sequence [] updates >>= fun _result ->
    Lwt.return ()
  in
  measured_loop do_one n >>= fun r ->
  report "deletes" r


let gets (client: Osd.osd) n value_size period prefix =
  let gen = make_key period prefix in
  let do_one i =
    let key = gen () in
    let open Slice in
    let key_slice = Slice.wrap_string key in
    client # get_option key_slice >>= fun _value ->
    let () =
      match _value with
      | None   -> failwith (Printf.sprintf "db[%s] = None?" key)
      | Some v -> assert (v.Slice.length = value_size)
    in
    Lwt.return ()
  in
  measured_loop do_one n >>= fun r ->
  report "gets" r

let sets (client:Osd.osd) n value_size period prefix =
  let gen = make_key period prefix in
  (* TODO: this affects performance as there is compression going
     on inside the database
   *)
  let open Slice in
  let value = Bytes.init
                value_size
                (fun i ->
                 let t0 = i+1 in
                 let t1 = t0 * t0 -1 in
                 let t2 = t1 mod 65535 in
                 let t3 = t2 mod 251 in
                 Char.chr t3)

  in
  let do_one i =
    let key = gen () in
    let key_slice = Slice.wrap_string key in
    let value_slice = Slice.wrap_string value in
    let open Checksum in
    let set = Update.Set (key_slice,
                          Some (value_slice, Checksum.NoChecksum, false))

    in
    let updates = [set] in
    client # apply_sequence [] updates >>= fun _result ->
    Lwt.return ()
  in
  measured_loop do_one n >>= fun r ->
  report "sets" r


let do_all client n value_size power prefix=
  let scenario = [
      sets;
      gets;
      deletes;
    ]
  in
  let period = period_of_power power in
  Lwt_list.iter_s
    (fun which ->
     which client
           n value_size period prefix
    )
    scenario
