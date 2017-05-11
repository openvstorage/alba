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

open! Prelude
open Remotes
open Lwt.Infix

class basic_nsm_host_pooled
    (mgr_access : Albamgr_client.client)
    nsm_hosts_pool
    forget
    nsm_host_id
  =

  let with_nsm_host_from_pool ~nsm_host_id f =
    let try_f () =
      Pool.Nsm_host.use_nsm_host
        nsm_hosts_pool
        ~nsm_host_id
        f
    in
    Lwt.catch
      try_f
      (let invalidate_nsm_host_info () =
         (* - clear cached info about this namespace
            - dispose pool
         *)
         forget nsm_host_id;
         Pool.Nsm_host.invalidate ~nsm_host_id nsm_hosts_pool
       in
       function
        | (Client_helper.MasterLookupResult.Error r) as exn->
          begin
            let clear_and_try_f () =
              invalidate_nsm_host_info ();
              try_f ()
            in
            let open Client_helper.MasterLookupResult in
            match r with
            | Found _ ->
              failwith "this doesn't result in an error"
            | No_master ->
              Lwt.fail exn
            | Too_many_nodes_down ->
              Lwt_log.info_f
                "Too many nodes down while connecting to nsm_host %S, this may or may not be related to an update of the nsm host config: clearing cached info and trying again"
                nsm_host_id >>= fun () ->
              clear_and_try_f ()
            | Unknown_node _ ->
              Lwt_log.info_f
                "Received an unknown master node while connecting to nsm_host %S, clearing cached info and trying again"
                nsm_host_id >>= fun () ->
              clear_and_try_f ()
            | Exception exn' ->
              invalidate_nsm_host_info ();
              Lwt.fail exn'
          end
        | exn ->
          begin match exn with
            | Nsm_model.Err.Nsm_exn _ -> ()
            | _ -> invalidate_nsm_host_info ()
          end;
          Lwt.fail exn)
  in

  object(self :# Nsm_host_client.basic_client)

    method query ?consistency command req =
      with_nsm_host_from_pool
        ~nsm_host_id
        (fun c -> c # query ?consistency command req)

    method update command req =
      with_nsm_host_from_pool
        ~nsm_host_id
        (fun c -> c # update command req)
  end

let gc_grace_period = 120.

class nsm_host_access
    (mgr : Albamgr_client.client)
    ~tls_config
    nsm_host_connection_pool_size
    buffer_pool
  =

  let nsm_hosts_info_cache = Hashtbl.create 3 in

  let get_nsm_host_info ~nsm_host_id =
    try Lwt.return (Hashtbl.find nsm_hosts_info_cache nsm_host_id)
    with Not_found ->
      mgr # get_nsm_host ~nsm_host_id
      >>= function
      | None ->
        Lwt.fail_with (Printf.sprintf "nsm host %S not found" nsm_host_id)
      | Some (_, info, lost) ->
        Hashtbl.replace nsm_hosts_info_cache nsm_host_id info;
        Lwt.return info
  in
  let forget nsm_host_id = Hashtbl.remove nsm_hosts_info_cache nsm_host_id in

  let nsm_hosts_pool =
    Pool.Nsm_host.make
      ~size:nsm_host_connection_pool_size
      ~tls_config
      (fun nsm_host_id -> get_nsm_host_info ~nsm_host_id)
      buffer_pool
  in

  let namespace_to_id_cache = Hashtbl.create 3 in
  let namespace_id_to_info_cache = Hashtbl.create 3 in

  let get_basic ~nsm_host_id =
    (* TODO pool met clients bijhouden *)
    new basic_nsm_host_pooled mgr nsm_hosts_pool forget nsm_host_id
  in

  let get ~nsm_host_id =
    (* TODO pool met clients bijhouden *)
    let basic = get_basic ~nsm_host_id in
    new Nsm_host_client.client basic
  in

  let get_nsm' ~namespace_id ~nsm_host_id =
    let c = get_basic ~nsm_host_id in
    new Nsm_client.client c namespace_id
  in

  let maybe_update_namespace_info ~namespace_id ns_info =

    let nsm_host_id =
      let open Albamgr_protocol.Protocol in
      ns_info.Namespace.nsm_host_id
    in
    let nsm = get_nsm' ~namespace_id ~nsm_host_id in

    nsm # list_all_active_osds >>= fun (_, osds) ->

    let get_gc_epoch () =
      nsm # get_gc_epochs
    in
    get_gc_epoch () >>= fun gc_epochs ->

    if Hashtbl.mem namespace_id_to_info_cache namespace_id
    then Lwt.return (Hashtbl.find namespace_id_to_info_cache namespace_id)
    else begin
      Lwt.ignore_result begin
        (* start a thread which periodically refreshes which osds
           are linked to this namespace and what the current gc_epoch is
           until the namespace gets deleted
           we only want 1 instance of this thread, so that's why
           the Hashtbl.mem a little above is there
        *)
        let rec inner () =
          Lwt_extra2.sleep_approx (gc_grace_period /. 2.) >>= fun () ->
          let open Albamgr_protocol.Protocol in
          Lwt.catch
            (fun () ->
               nsm # list_all_active_osds >>= fun (_, osds) ->
               get_gc_epoch () >>= fun gc_epochs ->
               Hashtbl.replace namespace_id_to_info_cache namespace_id (ns_info, osds, gc_epochs);
               Lwt.return `Continue)
            (function
              | Error.Albamgr_exn (Error.Namespace_does_not_exist, _)
              | Nsm_model.Err.Nsm_exn (Nsm_model.Err.Namespace_id_not_found, _) ->
                Lwt.return `Stop
              | exn ->
                Lwt_log.debug_f
                  ~exn
                  "Error in refresh namespace osds thread for namespace %Li, ignoring"
                  namespace_id >>= fun () ->
                Lwt.return `Continue) >>= function
          | `Stop ->
             Hashtbl.remove namespace_id_to_info_cache namespace_id;
             Lwt.return ()
          | `Continue ->
             inner ()
        in
        inner ()
      end;
      Hashtbl.replace namespace_id_to_info_cache namespace_id (ns_info, osds, gc_epochs);
      Lwt.return (ns_info, osds, gc_epochs)
    end
  in
  let next_id = ref 0L in
  let lock = Lwt_mutex.create () in
  let get_namespace_info ~namespace_id =
    try Lwt.return (Hashtbl.find namespace_id_to_info_cache namespace_id)
    with Not_found ->
      (* get namespace info in a batched manner *)
      let rec inner () =
        mgr # list_namespaces_by_id
            ~first:!next_id ~finc:true ~last:None
            ~max:100
        >>= fun ((cnt, namespaces), has_more) ->
        Lwt_list.iter_s
          (fun (namespace_id, _, namespace_info) ->
            next_id := namespace_id;
            Lwt_extra2.ignore_errors
              (fun () ->
                maybe_update_namespace_info ~namespace_id namespace_info >>= fun _ ->
                Lwt.return ()) >>= fun () ->
            Lwt.return ())
          namespaces >>= fun () ->
        if has_more
        then inner ()
        else Lwt.return_unit
      in
      Lwt_mutex.with_lock
        lock
        (fun () ->
          if namespace_id >= !next_id
          then inner ()
          else Lwt.return_unit)
      >>= fun () ->

      try Lwt.return (Hashtbl.find namespace_id_to_info_cache namespace_id)
      with Not_found ->
        (* fallback to old code in case item was cleared from cache *)
        mgr # get_namespace_by_id ~namespace_id
        >>= fun (_,namespace_name, namespace_info) ->
        maybe_update_namespace_info ~namespace_id namespace_info
  in
  let get_gc_epoch ~namespace_id =
    get_namespace_info ~namespace_id >>= fun (_, _, gc_epochs) ->
    let gc_epoch_o = Nsm_model.GcEpochs.get_latest_valid gc_epochs in
    Lwt.return (Option.get_some gc_epoch_o)
  in

  let rec with_namespace_id ~(namespace : string) f =
    let get_namespace_id_info () =
      mgr # get_namespace ~namespace
      >>= function
      | None ->
         Lwt_log.info_f "Nsm_host_access.with_namespace_id: namespace %S was not found" namespace >>= fun () ->
         Alba_client_errors.Error.(lwt_failwith NamespaceDoesNotExist)
      | Some (_,ns_info) ->
        let namespace_id = ns_info.Albamgr_protocol.Protocol.Namespace.id in
        Lwt.return (namespace_id, ns_info)
    in
    begin
      try Lwt.return (Hashtbl.find namespace_to_id_cache namespace)
      with Not_found ->
        get_namespace_id_info () >>= fun (namespace_id, ns_info) ->
        Hashtbl.replace namespace_to_id_cache namespace namespace_id;
        maybe_update_namespace_info ~namespace_id ns_info >>= fun _ ->
        Lwt.return namespace_id
    end >>= fun namespace_id ->
    Lwt.catch
      (fun () -> f namespace_id)
      (fun exn ->
         let id_may_have_changed =
           match exn with
           | Nsm_model.Err.Nsm_exn (err, _) -> begin
               let open Nsm_model.Err in
               match err with
               | Namespace_id_not_found
               | Unknown ->
                 true
               | _ ->
                 false
             end
           | Asd_protocol.Protocol.Error.Exn err -> begin
               let open Asd_protocol.Protocol.Error in
               match err with
               | Full -> false
               | _ -> true
             end
           | Lwt_unix.Timeout ->
             false
           | _ ->
             (* TODO special case more? or change the default? *)
             true
         in
         if id_may_have_changed
         then
           (get_namespace_id_info () >>= fun (namespace_id', namespace_info') ->
            let open Albamgr_protocol.Protocol in
            if namespace_info'.Namespace.state = Namespace.Creating
            then
              begin
                let nsm_host_id = namespace_info'.Namespace.nsm_host_id in
                Lwt_log.info_f
                  "Detected namespace %s:%Li in 'Creating' state, let's do some nsm_host message delivery towards %s"
                  namespace namespace_id'
                  nsm_host_id >>= fun () ->
                Alba_client_message_delivery_base.deliver_messages
                  mgr
                  Albamgr_protocol.Protocol.Msg_log.Nsm_host
                  nsm_host_id
                  ((get ~nsm_host_id) # deliver_messages) >>= fun () ->

                get_namespace_id_info () >>= fun (namespace_id', namespace_info') ->
                Lwt.return namespace_id'
              end
            else
              Lwt.return namespace_id') >>= fun namespace_id' ->

           if namespace_id = namespace_id'
           then
             begin
               Lwt_log.debug_f
                 ~exn
                 "Needlessly refreshed namespace_id as a result of an exception" >>= fun () ->
               Lwt.fail exn
             end
           else
             begin
               Lwt_log.info_f
                 ~exn
                 "Retrying operation as the cached namespace_id had become invalid" >>= fun () ->
               Hashtbl.remove namespace_to_id_cache namespace;
               with_namespace_id ~namespace f
             end
         else
           Lwt.fail exn
      )
  in

  let refresh_namespace_osds ~namespace_id =
    get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
    let nsm = get_nsm'
        ~namespace_id
        ~nsm_host_id:ns_info.Albamgr_protocol.Protocol.Namespace.nsm_host_id in

    nsm # list_all_active_osds >>= fun osds ->

    (* update the cache with the newly found osds *)
    (match Hashtbl.find namespace_id_to_info_cache namespace_id with
     | exception Not_found -> ()
     | (ns_info, _, gc_epochs) ->
       Hashtbl.replace namespace_id_to_info_cache namespace_id (ns_info, snd osds, gc_epochs));

    Lwt.return osds
  in

  let get_nsm_by_id ~namespace_id =
    get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
    let nsm_host_id = ns_info.Albamgr_protocol.Protocol.Namespace.nsm_host_id in
    let nsm_host_basic = get_basic ~nsm_host_id in
    let nsm_client = new Nsm_client.client nsm_host_basic namespace_id in
    Lwt.return nsm_client
  in

  object(self)
    method finalize = Pool.Nsm_host.invalidate_all nsm_hosts_pool

    method get = get
    method get_basic = get_basic

    method get_nsm_host_info = get_nsm_host_info

    method get_nsm_by_id = get_nsm_by_id

    method get_namespace_info = get_namespace_info
    method get_gc_epoch = get_gc_epoch

    method with_namespace_id :
      type a. namespace : string -> (int64 -> a Lwt.t) -> a Lwt.t =
      with_namespace_id
    method with_nsm_client :
      type a. namespace : string -> (Nsm_client.client -> a Lwt.t) -> a Lwt.t =
      fun ~namespace f ->
        with_namespace_id
          ~namespace
          (fun namespace_id ->
             get_nsm_by_id ~namespace_id >>= fun nsm_client ->
             f nsm_client)

      method with_nsm_host_client :
      type a. nsm_host_id : string -> (Nsm_host_client.single_connection_client -> a Lwt.t) -> a Lwt.t =
        fun ~nsm_host_id f ->
        Remotes.Pool.Nsm_host.use_nsm_host
          nsm_hosts_pool
          ~nsm_host_id
          f

    method refresh_namespace_osds = refresh_namespace_osds
    method maybe_update_namespace_info = maybe_update_namespace_info
    method statistics nsm_host_id clear =
      (self # get ~nsm_host_id) # statistics clear
  end
