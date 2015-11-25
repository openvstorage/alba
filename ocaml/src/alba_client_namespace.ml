(*
Copyright 2015 iNuron NV

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

open Lwt.Infix

let create_namespace
      mgr_access
      get_preset_info
      deliver_messages_to_most_osds
      deliver_nsm_host_messages
      ~namespace ~preset_name ?nsm_host_id () =
  Lwt_log.debug_f "Alba_client: create_namespace %S" namespace >>= fun () ->

  let t0 = Unix.gettimeofday () in

  mgr_access #  create_namespace ~namespace ~preset_name ?nsm_host_id ()
  >>= fun ns_info ->

  let open Albamgr_protocol.Protocol in
  let namespace_id = ns_info.Namespace.id in
  let nsm_host_id = ns_info.Namespace.nsm_host_id in

  let deliver_nsm_host_messages () =
    (* message delivery can interfere with message delivery
     from e.g. the maintenance process, that's why we're
     ignoring errors here *)
    Lwt_extra2.ignore_errors
      (fun () -> deliver_nsm_host_messages ~nsm_host_id)
  in

  deliver_nsm_host_messages () >>= fun () ->

  let t1 = Unix.gettimeofday () in

  Lwt.choose
    [ Lwt_unix.sleep (4.0 -. (t1 -. t0));
      begin
        get_preset_info ~preset_name:ns_info.Namespace.preset_name >>= fun preset ->

        mgr_access # list_all_namespace_osds ~namespace_id >>= fun (_, osds) ->

        let nsm_host_delivery_thread = ref None in
        let need_more_delivery = ref false in
        let ensure_delivery () =
          match !nsm_host_delivery_thread with
          | None ->
             let rec inner () =
               deliver_nsm_host_messages () >>= fun () ->
               if !need_more_delivery
               then
                 begin
                   need_more_delivery := false;
                   inner ()
                 end
               else
                 Lwt.return ()
             in
             nsm_host_delivery_thread :=
               Some (Lwt.finalize
                       inner
                       (fun () -> nsm_host_delivery_thread := None;
                                  Lwt.return ()));
          | Some _ ->
             need_more_delivery := true
        in

        deliver_messages_to_most_osds
          ~osds ~preset
          ~delivered:ensure_delivery >>= fun () ->
        match !nsm_host_delivery_thread with
        | Some t -> t
        | None -> Lwt.return ()
      end; ] >>= fun () ->

  Lwt_log.debug_f "Alba_client: create_namespace %S:%li ok"
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
  Lwt_log.debug_f "Alba_client: delete_namespace %S" namespace >>= fun () ->

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
