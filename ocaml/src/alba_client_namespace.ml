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

open Lwt.Infix

let create_namespace
      mgr_access
      (nsm_host_access : Nsm_host_access.nsm_host_access)
      get_preset_info
      deliver_messages_to_most_osds
      deliver_nsm_host_messages
      ~namespace ~preset_name ?nsm_host_id () =
  Lwt_log.info_f "Alba_client: create_namespace %S" namespace >>= fun () ->

  let t0 = Unix.gettimeofday () in

  mgr_access #  create_namespace ~namespace ~preset_name ?nsm_host_id ()
  >>= fun ns_info ->

  Lwt_log.info_f "Alba_client: namespace %S created on albamgr" namespace >>= fun () ->


  let open Albamgr_protocol.Protocol in
  let namespace_id = ns_info.Namespace.id in
  let nsm_host_id = ns_info.Namespace.nsm_host_id in

  let deliver_nsm_host_messages () =
    (* message delivery can interfere with message delivery
     from e.g. the maintenance process, that's why we're
     ignoring errors here *)
    Lwt_extra2.ignore_errors
      ~logging:true
      (fun () -> deliver_nsm_host_messages ~nsm_host_id)
  in

  deliver_nsm_host_messages () >>= fun () ->

  Lwt_log.info_f "Alba_client: create namespace %S: nsm host messages delivered to %s"
                 namespace nsm_host_id >>= fun () ->


  let t1 = Unix.gettimeofday () in

  Lwt.choose
    [ Lwt_unix.sleep (4.0 -. (t1 -. t0));
      begin
        get_preset_info ~preset_name:ns_info.Namespace.preset_name >>= fun preset ->

        mgr_access # list_all_namespace_osds ~namespace_id >>= fun (_, osds) ->

        Lwt_log.info_f "Alba_client: create namespace %S: delivering osd messages"
                       namespace >>= fun () ->


        let nsm_host_delivery_thread = ref None in
        let need_more_delivery = ref false in
        let ensure_nsm_host_delivery () =
          match !nsm_host_delivery_thread with
          | None ->
             let rec inner () =
               Lwt.catch
                 deliver_nsm_host_messages
                 (fun exn ->
                  Lwt_log.info_f
                    ~exn
                    "An exception occured while delivering nsm_host messages during create namespace")
               >>= fun () ->
               if !need_more_delivery
               then
                 begin
                   need_more_delivery := false;
                   inner ()
                 end
               else
                 begin
                   nsm_host_delivery_thread := None;
                   nsm_host_access # refresh_namespace_osds ~namespace_id >>= fun _ ->
                   Lwt.return_unit
                 end
             in
             nsm_host_delivery_thread := Some (inner ());
          | Some _ ->
             need_more_delivery := true
        in

        deliver_messages_to_most_osds
          ~osds ~preset
          ~delivered:ensure_nsm_host_delivery >>= fun () ->

        let wait_for_nsm_host_delivery_thread () =
          match !nsm_host_delivery_thread with
          | Some t -> t
          | None -> Lwt.return ()
        in

        wait_for_nsm_host_delivery_thread () >>= fun () ->
        deliver_nsm_host_messages ()
      end; ] >>= fun () ->

  Lwt_log.info_f "Alba_client: create_namespace %S:%Li ok"
                  namespace namespace_id
  >>= fun () ->
  Lwt.return namespace_id

let delete_namespace
      mgr_access
      (nsm_host_access : Nsm_host_access.nsm_host_access)
      deliver_nsm_host_messages
      drop_cache_by_id
      ~namespace
  =
  Lwt_log.info_f "Alba_client: delete_namespace %S" namespace >>= fun () ->

  nsm_host_access # with_namespace_id
    ~namespace
    (fun namespace_id ->

     Lwt.ignore_result begin
         Lwt_extra2.ignore_errors
           (fun () -> drop_cache_by_id namespace_id)
       end;

     mgr_access # list_all_namespace_osds ~namespace_id >>= fun (_, osds) ->

     mgr_access # delete_namespace ~namespace >>= fun nsm_host_id ->

     deliver_nsm_host_messages ~nsm_host_id)
