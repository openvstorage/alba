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

class alba_bench_client alba_client =
object
  method write_object_fs
        ~namespace ~object_name
        ~input_file
        ~allow_overwrite
        ?(checksum = None) () =
    alba_client # upload_object_from_file
                ~namespace
                ~object_name ~input_file
                ~checksum_o:checksum
                ~allow_overwrite:(let open Nsm_model in
                                  if allow_overwrite
                                  then Unconditionally
                                  else NoPrevious)
    >>= fun _ ->
    Lwt.return ()

    method read_object_fs
      ~namespace ~object_name
      ~output_file
      ~consistent_read
      ~should_cache
      =
      alba_client # download_object_to_file
                  ~namespace
                  ~object_name
                  ~output_file
                  ~consistent_read ~should_cache
      >>= function
      | None -> let open Proxy_protocol.Protocol in
                Error.(failwith ObjectDoesNotExist)
      | Some _ -> Lwt.return ()

    method read_object_slices ~namespace ~object_slices ~consistent_read =
      Proxy_server.read_objects_slices
        alba_client
        namespace object_slices ~consistent_read

    method delete_object ~namespace ~object_name ~may_not_exist=
      alba_client # delete_object ~namespace ~object_name ~may_not_exist

    method get_version = Lwt.return Alba_version.summary
end

let do_scenarios
      albamgr_cfg
      n_clients n
      file_name power prefix client_file_prefix upload_slack
      slice_size namespace
      scenarios =
  let period = period_of_power power in
  Alba_client2.with_client
    ~tcp_keepalive:Tcp_keepalive2.default
    ~populate_osds_info_cache:true
    ~upload_slack
    albamgr_cfg
    (fun alba_client ->
      let client = new alba_bench_client alba_client in
      Lwt_list.map_p
        (fun i ->
          let client_fn =
            Printf.sprintf
              "./%s.client_%03i.out"
              client_file_prefix
              i
          in
          Lwt_io.open_file ~mode:Lwt_io.output
                           ~flags:[Unix.O_WRONLY; Unix.O_CREAT; Unix.O_APPEND; Unix.O_NONBLOCK]
                           client_fn >>= fun oc ->
          Lwt.return (i,oc)
        )
        (Int.range 0 n_clients)
      >>= fun i_files ->

      Lwt.finalize
        (fun () ->
          Lwt_list.iter_s
            (fun scenario ->
              let step = max (n / 100) 1 in
              let n_per_client = n / n_clients in
              Lwt_list.iter_p
                (fun (i,oc) ->
                  let progress = make_progress step in
                  scenario
                    ~oc
                    client
                    progress
                    n_per_client
                    file_name
                    period
                    (Printf.sprintf "%s_%i" prefix i)
                    slice_size
                    namespace)
                i_files)
            scenarios)
        (fun () ->
          Lwt_list.iter_p
            (fun (i,oc) ->
              Lwt.catch
                (fun () -> Lwt_io.close oc)
                (fun _ -> Lwt.return_unit)
            ) (i_files)

        )
    )
