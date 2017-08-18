(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Slice
open Lwt.Infix

let recent_enough target = function
  | [] -> false
  | timestamp :: _ -> timestamp > target

let periodic_load_osds
      (alba_client : Alba_base_client.client)
      maintenance_config osds_with_state =
  let past_date =
    Unix.gettimeofday () -.
      (maintenance_config.Maintenance_config.auto_repair_timeout_seconds
       /. 2.) in
  let open Nsm_model in
  let do_one (osd_id, osd_info, osd_state) =
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
                Lwt_log.info_f "Write load on %Li" osd_id >>= fun () ->
                write_test_blob ()
                >>= function
                | Ok _ | Error Osd.Error.Full  ->
                   Osd_state.add_write osd_state;
                   Lwt_log.info_f "Write load on %Li succeeded" osd_id
                | Error _ ->
                   Lwt.return_unit)
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
                Lwt_log.info_f "Read load on %Li" osd_id >>= fun () ->
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
                     Lwt_log.info_f "Read load on %Li succeeded" osd_id
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
    osds_with_state
