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
open Lwt.Infix
open Generic_bench

let do_writes
      ~robust ~oc
      client progress n input_files
      ~seed ~period
      prefix _ namespace
  =
  let gen = make_key ~seed ~period prefix in
  let no_files = List.length input_files in
  if no_files = 0
  then failwith "please specify files for 'writes' scenario";
  let do_one i =
    let object_name = gen () in
    let input_file = List.nth_exn input_files (i mod no_files) in
    let checksum : Checksum.Checksum.t option option = None in
    Lwt.catch
      (fun () ->
       client # write_object_fs
              ~namespace
              ~object_name
              ~input_file
              ~allow_overwrite:false
              ?checksum ())
      (fun exn ->
       let rec inner delay =
         Lwt.catch
           (fun () ->
            client # write_object_fs
                   ~namespace
                   ~object_name
                   ~input_file
                   ~allow_overwrite:true
                   ?checksum () >>= fun () ->
            Lwt.return `Continue)
           (fun exn ->
            Lwt.return `Retry) >>= function
         | `Continue ->
            Lwt.return ()
         | `Retry ->
            Lwt_unix.sleep delay >>= fun () ->
            inner (min 60. (delay *. 1.5))
       in
       if robust
       then inner 1.
       else Lwt.fail exn)
  in
  measure_and_report oc progress do_one n "writes"


let do_reads
      ~oc
      client
      progress n _
      ~seed ~period prefix
      (_:int) namespace =
  let gen = make_key ~seed ~period prefix in
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
  measure_and_report oc progress do_one n "reads"

let do_partial_reads
      ~oc
      client
      progress n _
      ~seed ~period prefix
      (slice_size:int) namespace =
  let gen = make_key ~seed ~period prefix in
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
  measure_and_report oc progress do_one n "partial reads"

let do_deletes
      ~oc
      client
      progress n _
      ~seed ~period prefix
      (_:int) namespace =
  let gen = make_key ~seed ~period prefix in
  let do_one i =
    let object_name = gen ()  in
    client # delete_object ~namespace ~object_name ~may_not_exist:false
  in
  measure_and_report oc progress do_one n "deletes"

let do_get_version
      ~oc
      client
      progress n _
      ~seed ~period
      _prefix _ _namespace =
  let do_one _ =
    client # get_version >>= fun _ ->
    Lwt.return_unit
  in
  measure_and_report oc progress do_one n "get_version"


let do_scenarios
      host port transport
      n_clients n
      file_names power prefix slice_size namespace
      scenarios (seeds:int list)
  =
  let period = period_of_power power in

  Lwt_list.iter_s
    (fun scenario ->
      let step = max (n / 100) 1 in
      let progress = make_progress step  in
      Lwt_list.iter_p
        (fun i ->
          let seed : int = List.nth seeds i |> Option.get_some
          and prefix = Printf.sprintf "%s_%i" prefix i
          in
          Proxy_client.with_client
            host port transport
            (fun client ->
              let oc = Lwt_io.stdout in
              let per_client = n / n_clients in
              scenario
                ~oc
                client
                progress
                per_client
                file_names
                ~seed
                ~period
                prefix
                slice_size
                namespace
        ))
        (Int.range 0 n_clients))
    scenarios
