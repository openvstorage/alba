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
open Slice
open Lwt.Infix

let recent_enough target = function
  | [] -> false
  | timestamp :: _ -> timestamp > target

let periodic_load_osds
      (alba_client : Alba_base_client.client)
      maintenance_config osds =
  let past_date =
    Unix.gettimeofday () -.
      (maintenance_config.Maintenance_config.auto_repair_timeout_seconds
       /. 2.) in
  let open Nsm_model in
  let do_one (osd_id, osd_info) =
        alba_client # osd_access # get_osd_info ~osd_id
        >>= fun (_, osd_state,_) ->

        let write_test_blob () =
          alba_client # with_osd
                      ~osd_id
                      (fun osd ->
                       osd # global_kvs # apply_sequence
                           Osd.Low
                           []
                           [ Osd.Update.set_string
                               Osd_keys.test_key
                               (Lazy.force Osd_access_type.large_value)
                               Checksum.NoChecksum
                               false ])
        in

        (if not (recent_enough past_date osd_info.OsdInfo.write)
         then
           begin
             Lwt.catch
               (fun () ->
                Lwt_log.debug_f "Write load on %li" osd_id >>= fun () ->
                write_test_blob ()
                >>= function
                | Osd.Ok ->
                   Osd_state.add_write osd_state;
                   Lwt.return ()
                | Osd.Exn _ ->
                   Lwt.return ())
               (fun exn ->
                Lwt.return ())
           end
         else
           Lwt.return ())
        >>= fun () ->
        (if not (recent_enough past_date osd_info.OsdInfo.read)
         then
           begin
             Lwt_extra2.ignore_errors
               (fun () ->
                Lwt_log.debug_f "Read load on %li" osd_id >>= fun () ->
                let rec inner () =
                  alba_client # with_osd
                              ~osd_id
                              (fun osd ->
                               osd # global_kvs # get_option
                                   Osd.Low
                                   (Slice.wrap_string Osd_keys.test_key))
                  >>= function
                  | Some _ ->
                     Osd_state.add_read osd_state;
                     Lwt.return ()
                  | None ->
                     (* seems like the test blob was not yet available on the osd
                      * so let's put it there and retry... *)
                     write_test_blob () >>= fun _ ->
                     inner ()
                in
                inner ())
           end
         else
           Lwt.return ())
  in
  Lwt_list.iter_p
    (fun o ->
     Lwt_extra2.ignore_errors
       (fun () -> do_one o))
    osds
