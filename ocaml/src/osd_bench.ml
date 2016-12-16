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
open Asd_protocol
open Lwt.Infix
open Generic_bench
open Checksum
open Slice

let oc = Lwt_io.stdout

let maybe_fail = function
  | Osd.Ok -> Lwt.return_unit
  | Osd.Exn e -> Osd.Error.lwt_fail e

let deletes with_client progress n value_size _
            ~seed ~period prefix =

  let run client =
      let gen = make_key ~seed ~period prefix in
      let do_one i =
        let key = gen () in
        let key_slice = Slice.wrap_string key in
        let delete = Update.Set (key_slice, None) in
        let updates = [delete] in
        client # global_kvs # apply_sequence
               Osd.High [] updates >>= maybe_fail
      in
      measured_loop oc progress do_one n >>= fun r ->
      report oc "deletes" r
  in
  with_client run


let gets with_client progress n value_size _
         ~seed ~period prefix =
  let run client =
      let gen = make_key ~seed ~period prefix in
      let do_one i =
        let key = gen () in
        let key_slice = Slice.wrap_string key in
        client # global_kvs # get_option Osd.High key_slice >>= fun _value ->
        let () =
          match _value with
          | None   -> failwith (Printf.sprintf "db[%s] = None?" key)
          | Some v -> assert (Lwt_bytes.length v = value_size)
        in
        Lwt.return ()
      in
      measured_loop oc progress do_one n >>= fun r ->
      report oc "gets" r
  in
  with_client run


let partial_reads with_client progress n _value_size partial_fetch_size
                  ~seed ~period prefix =
  let run client =
    let gen = make_key ~seed ~period prefix in
    let target = Lwt_bytes.create partial_fetch_size in
    let do_one i =
      let key = gen () in
      client # global_kvs # partial_get
             Osd.High
             (Slice.wrap_string key)
             [ 0, partial_fetch_size, target, 0 ] >>= function
      | Osd.Unsupported -> failwith "partial read not supported"
      | Osd.NotFound -> failwith "partial read key not found"
      | Osd.Success -> Lwt.return_unit
    in
    measured_loop oc progress do_one n >>= fun r ->
    report oc "partial_reads" r
  in
  with_client run


let get_version with_client progress n _ _ ~seed ~period _ =
  let run client =
    let do_one _ =
      client # get_version >>= fun _ ->
      Lwt.return ()
    in
    measured_loop oc progress do_one n >>= fun r ->
    report oc "get_version" r
  in
  with_client run

let churn with_client progress n _ _ ~seed ~period _ =
  let do_one i =
    with_client
      (fun client ->
        client # get_disk_usage >>= fun (used, total) ->
        Lwt.catch
          (fun () ->
            client # capabilities >>= fun _ ->
            Lwt.return_unit
          )
          (fun exn -> Lwt.fail exn)
      )
  in
  measured_loop oc progress do_one n >>= fun r ->
  report oc "churn" r


let exists with_client progress n _ _ ~seed ~period prefix =
  let run client =
    let gen = make_key ~seed ~period prefix in
    let do_one i =
      let key = gen () in
      client # global_kvs # multi_exists
             Osd.High
             [ (Slice.wrap_string key) ]
      >>= fun _ ->
      Lwt.return_unit
    in
    measured_loop oc progress do_one n >>= fun r ->
    report oc "exists" r
  in
  with_client run


let _make_value value_size =
  (* TODO: this affects performance as there is compression going
     on inside the database
   *)
  Bytes.init
    value_size
    (fun i ->
     let t0 = i+1 in
     let t1 = t0 * t0 -1 in
     let t2 = t1 mod 65535 in
     let t3 = t2 mod 251 in
     Char.chr t3)

let sets with_client progress n value_size _
         ~seed ~period prefix =
  let run client =
    let gen = make_key ~seed ~period prefix in

    let value = _make_value value_size in

    let do_one i =
      let key = gen () in
      let key_slice = Slice.wrap_string key in
      let set = Update.Set (key_slice,
                            Some (Blob.Bytes value,
                                  Checksum.NoChecksum,
                                  false))
      in
      let updates = [set] in
      client # global_kvs # apply_sequence Osd.High [] updates >>= maybe_fail
    in
    measured_loop oc progress do_one n >>= fun r ->
    report oc "sets" r
  in
  with_client run

let range_queries with_client progress n value_size _ ~seed ~period prefix =
  let run client =
    let gen = make_key ~seed ~period prefix in
    let do_one i =
      let first_key = gen () in
      let first = Slice.wrap_string first_key in
      client # global_kvs # range Osd.High
             ~first ~finc:false ~last:None ~reverse:false ~max:100
      >>= fun keys ->
      Lwt.return ()
    in
    measured_loop oc progress do_one n >>= fun r ->
    report oc "ranges" r
  in
  with_client run

let upload_fragments with_client progress n value_size _
                     ~seed ~period prefix =
  let run client =
    let gen = make_key ~seed ~period prefix in
    let open Osd_keys in
    let value = _make_value value_size in
    let value_blob = Blob.Bytes value in
    let namespace_id = 0xffff_ffff_ffff_ffffL in
    let namespace_status_key = AlbaInstance.namespace_status ~namespace_id in
    let active_value = Osd.Osd_namespace_state.(serialize
                                                  to_buffer
                                                  Active)
    in
    let assert_namespace_active =
      Osd.Assert.value_string
        namespace_status_key active_value
    in
    (client # namespace_kvs namespace_id) # apply_sequence
                                          Osd.High
                                          []
                                          [Osd.Update.set
                                             (Slice.wrap_string namespace_status_key)
                                             (Blob.Bytes active_value)
                                             Checksum.NoChecksum
                                             false;
                                          ]
    >>= maybe_fail
    >>= fun () ->
    let recovery_info = Blob.Bytes (_make_value 134) in
    let do_one i =
      let key = gen () in
      let object_id = key in
      let version_id = 0 in
      let chunk_id = 0 in
      let fragment_id = 0 in
      let set_data = Osd.Update.set
                       (AlbaInstance.fragment
                          ~object_id ~version_id
                          ~chunk_id ~fragment_id
                        |> Slice.wrap_string)
                       value_blob
                       Checksum.NoChecksum
                       false
      in
      let set_recovery_info =
        Osd.Update.set
          (Slice.wrap_string
             (AlbaInstance.fragment_recovery_info
                ~object_id ~version_id
                ~chunk_id ~fragment_id))
          recovery_info Checksum.NoChecksum true
      in
      let gc_epoch = 0L in
      let set_gc_tag =
        Osd.Update.set_string
          (AlbaInstance.gc_epoch_tag
             ~gc_epoch
             ~object_id ~version_id
             ~chunk_id ~fragment_id)
          "" Checksum.NoChecksum true
      in
      let asserts = [ assert_namespace_active; ]
      and updates = [ set_data;
                      set_recovery_info;
                      set_gc_tag; ]
      in
      (client # namespace_kvs namespace_id) # apply_sequence
                                            Osd.High
                                            asserts updates >>= maybe_fail
    in
    measured_loop oc progress do_one n >>= fun r ->
    report oc "uploads" r
  in
  with_client run




let do_scenarios
      with_client
      n_clients n
      value_size partial_fetch_size power prefix
      seeds
      scenarios
  =
  let period = period_of_power power in
  Lwt_list.iter_s
    (fun scenario ->
      let step = max (n/100) 1 in
      let progress = make_progress step in
      Lwt_list.iter_p
        (fun i ->
          let seed = List.nth seeds i |> Option.get_some in
          scenario
            with_client
            progress
            (n/n_clients)
            value_size
            partial_fetch_size
            ~seed
            ~period
            (Printf.sprintf "%s_%i" prefix i)
        )
        (Int.range 0 n_clients)
    )
    scenarios
