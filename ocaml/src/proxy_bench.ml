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

open Proxy_client
open Lwt.Infix
open Generic_bench


let do_writes (client:proxy_client) n input_file period prefix (_:int) namespace =
  let gen = make_key period prefix in
  let do_one i =
    let object_name = gen () in
    client # write_object_fs
           ~namespace
           ~object_name
           ~input_file
           ~allow_overwrite:false ()
  in
  Lwt_io.printlf "writes:" >>= fun () ->
  measured_loop do_one n >>= fun r ->
  report "writes" r

let do_reads (client:proxy_client) n _ period prefix (_:int) namespace =
  let gen = make_key period prefix in
  let do_one i =
    let object_name = gen () in
    let output_file = Printf.sprintf "/tmp/%s_%s.tmp" namespace prefix in
    client # read_object_fs
           ~namespace
           ~object_name
           ~output_file
           ~consistent_read:false
           ~should_cache:true
  in
  Lwt_io.printlf "reads:" >>= fun () ->
  measured_loop do_one n >>= fun r ->
  report "reads" r

let do_partial_reads (client:proxy_client) n _ period prefix (slice_size:int) namespace =
  let gen = make_key period prefix in
  let do_one i =
    let object_name = gen () in
    let object_slices = [
        (object_name,[0L, slice_size]); (* TODO: what with smaller objects? *)
      ]
    in
    client # read_object_slices ~namespace ~object_slices ~consistent_read:false
    >>= fun _data ->
    Lwt.return ()
  in
  Lwt_io.printlf "partial reads:" >>= fun () ->
  measured_loop do_one n >>=fun r ->
  report "partial reads" r

let do_deletes (client:proxy_client) n _ period prefix (_:int) namespace =
  let gen = make_key period prefix in
  let do_one i =
    let object_name = gen ()  in
    client # delete_object ~namespace ~object_name ~may_not_exist:false
  in
  Lwt_io.printlf "deletes:" >>= fun () ->
  measured_loop do_one n >>= fun r ->
  report "deletes" r

let do_all client n file_name power prefix slice_size namespace =
  let scenario = [
      do_writes;
      do_reads;
      do_partial_reads;
      do_deletes;
    ]
  in
  let period = period_of_power power in
  Lwt_list.iter_s
    (fun which -> which client n file_name period prefix slice_size namespace) scenario

let bench host port n file_name power prefix slice_size namespace =
  Proxy_client.with_client
    host port
    (fun client -> do_all client n file_name power prefix slice_size namespace)
