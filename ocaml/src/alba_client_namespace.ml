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

open Lwt.Infix

let create_namespace
      mgr_access
      get_preset_info
      deliver_messages_to_most_osds
      deliver_nsm_host_messages
      ~namespace ~preset_name ?nsm_host_id () =
  Lwt_log.debug_f "Alba_client: create_namespace %S" namespace >>= fun () ->
  mgr_access #  create_namespace ~namespace ~preset_name ?nsm_host_id ()
  >>= fun ns_info ->

  let open Albamgr_protocol.Protocol in
  let namespace_id = ns_info.Namespace.id in
  let nsm_host_id = ns_info.Namespace.nsm_host_id in

  get_preset_info ~preset_name:ns_info.Namespace.preset_name >>= fun preset ->

  let osds_t =
    mgr_access # list_all_namespace_osds ~namespace_id >>= fun (_, osds) ->
    deliver_messages_to_most_osds ~osds ~preset
  in

  osds_t >>= fun () ->

  (* message delivery can interfere with message delivery
     from e.g. the maintenance process, that's why we're
     ignoring errors here *)
  Lwt_extra2.ignore_errors
    (fun () -> deliver_nsm_host_messages ~nsm_host_id) >>= fun () ->

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
