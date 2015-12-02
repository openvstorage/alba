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

open Prelude
open Lwt
open Albamgr_protocol
open Protocol

class type basic_client = object
  method query : 'i 'o.
                 ?consistency : Consistency.t ->
                 ('i, 'o) query -> 'i -> 'o Lwt.t

  method update : 'i 'o.
                  ('i, 'o) update -> 'i -> 'o Lwt.t
end

class client (client : basic_client) =
  object(self)
    val supports_update_osds = ref None
    val supports_mark_msgs_delivered = ref None

    method list_nsm_hosts ~first ~finc ~last ~max ~reverse =
      client # query
        ListNsmHosts
        RangeQueryArgs.{ first; finc; last; max; reverse; }

    method get_nsm_host ~nsm_host_id =
      self # list_nsm_hosts
        ~first:nsm_host_id ~finc:true
        ~last:(Some (nsm_host_id, true))
        ~max:1 ~reverse:false >>= fun ((_, nsm_hosts), _) ->
      Lwt.return (List.hd nsm_hosts)

    method list_all_nsm_hosts () =
      list_all_x
        ~first:""
        (fun (id, _, _) -> id)
        (self # list_nsm_hosts ~last:None ~max:(-1) ~reverse:false)

    method list_namespaces_by_id ~first ~finc ~last ~max=
      let args = RangeQueryArgs.{
                   first;
                   finc;
                   last;
                   reverse = false;
                   max;
                 }
      in
      client # query ListNamespacesById args

    method add_nsm_host ~nsm_host_id ~nsm_host_info =
      client # update
        AddNsmHost
        (nsm_host_id, nsm_host_info)

    method update_nsm_host ~nsm_host_id ~nsm_host_info =
      client # update
        UpdateNsmHost
        (nsm_host_id, nsm_host_info)

    method create_namespace ~namespace ~preset_name ?nsm_host_id () =
      begin match nsm_host_id with
        | None ->
          self # list_all_nsm_hosts () >>= fun (count,r) ->

          let active_nsm_hosts =
            List.filter
              (fun (_, info, _) -> not info.Nsm_host.lost)
              r
          in

          if active_nsm_hosts = [] then failwith "0 active nsm hosts";

          let id, _, _ =
            List.fold_left
              (fun ((_, _, bc) as best) ((_, _, cc) as cur) ->
                 if cc < bc
                 then cur
                 else best)
              (List.hd_exn active_nsm_hosts)
              (List.tl_exn active_nsm_hosts)
          in

          Lwt.return id
        | Some nsm_host_id ->
          Lwt.return nsm_host_id
      end >>= fun nsm_host_id ->
      client # update
        CreateNamespace
        (namespace, nsm_host_id, preset_name)

    method delete_namespace ~namespace =
      client # update DeleteNamespace namespace

    method list_namespaces ~first ~finc ~last ~max ~reverse =
      client # query
        ListNamespaces
        RangeQueryArgs.{ first; finc; last; max; reverse; }

    method get_namespace ~namespace =
      self # list_namespaces
        ~first:namespace ~finc:true ~last:(Some (namespace, true))
        ~max:1 ~reverse:false >>= fun ((_, namespaces), _) ->
      Lwt.return (List.hd namespaces)

    method get_namespace_by_id ~namespace_id =
      self # list_namespaces_by_id
        ~first:namespace_id
        ~finc:true
        ~last:(Some (namespace_id,true))
        ~max:1 >>= fun ((_, namespaces),_) ->
      match namespaces with
      | [] -> Error.(failwith_lwt Error.Namespace_does_not_exist)
      | [x] -> Lwt.return x
      | _ -> Lwt.fail_with "nou moe"


    method list_all_namespaces =
      list_all_x
        ~first:""
        fst
        (self # list_namespaces
           ~last:None
           ~max:(-1) ~reverse:false)

    method list_namespace_osds ~namespace_id ~first ~finc ~last ~max ~reverse =
      client # query
        ListNamespaceOsds
        (namespace_id,
         RangeQueryArgs.({
             first; finc; last;
             max; reverse;
           }))

    method list_all_namespace_osds ~namespace_id =
      list_all_x
        ~first:0l
        fst
        (self # list_namespace_osds
           ~namespace_id
           ~last:None
           ~max:(-1) ~reverse:false)

    method create_preset preset_name preset =
      client # update
        CreatePreset
        (preset_name, preset)

    method set_default_preset preset_name =
      client # update
        SetDefaultPreset
        preset_name

    method delete_preset preset_name =
      client # update DeletePreset preset_name

    method list_presets ~first ~finc ~last ~reverse ~max =
      client # query
        ListPresets
        RangeQueryArgs.({ first; finc; last; reverse; max; })

    method list_all_presets () =
      list_all_x
        ~first:""
        (fun (name, _, _, _) -> name)
        (self # list_presets
           ~last:None
           ~reverse:false ~max:(-1))

    method get_preset ~preset_name =
      self # list_presets
        ~first:preset_name ~finc:true ~last:(Some(preset_name, true))
        ~max:1 ~reverse:false >>= fun ((_, presets), _) ->
      Lwt.return (List.hd presets)

    method add_osds_to_preset ~preset_name ~osd_ids =
      client # update
        AddOsdsToPreset
        (preset_name, (List.length osd_ids, osd_ids))

    method list_osds_by_osd_id ~first ~finc ~last ~reverse ~max =
      client # query
        ListOsdsByOsdId
        RangeQueryArgs.({ first; finc; last; reverse; max; })

    method get_osd_by_osd_id ~osd_id =
      self # list_osds_by_osd_id
        ~first:osd_id ~finc:true
        ~last:(Some(osd_id, true))
        ~max:1 ~reverse:false >>= fun ((_, osds), _) ->
      Lwt.return (Option.map snd (List.hd osds))

    method list_osds_by_long_id ~first ~finc ~last ~reverse ~max =
      client # query
        ListOsdsByLongId
        RangeQueryArgs.({ first; finc; last; reverse; max; })

    method get_osd_by_long_id ~long_id =
      self # list_osds_by_long_id
        ~first:long_id ~finc:true ~last:(Some(long_id, true))
        ~max:1 ~reverse:false >>= fun ((_, osds), _) ->
      Lwt.return (List.hd osds)

    method list_all_osds =
      list_all_x
        ~first:""
        (fun (_, osd_info) -> Osd.get_long_id osd_info.Osd.kind)
        (self # list_osds_by_long_id
           ~last:None
           ~reverse:false ~max:(-1))

    method list_all_claimed_osds =
      list_all_x
        ~first:0l
        fst
        (self # list_osds_by_osd_id
           ~last:None
           ~reverse:false ~max:(-1))

    method list_available_osds =
      client # query ListAvailableOsds ()

    method add_osd osd_info =
      client # update AddOsd osd_info

    method update_osd ~long_id changes =
      client # update UpdateOsd (long_id, changes)

    method update_osds updates =
      let do_it () = client # update UpdateOsds updates in
      let fake_it () =
        Lwt_list.fold_left_s
          (fun () (long_id,changes) ->self # update_osd ~long_id changes)
          () updates
      in
      match !supports_update_osds with
      | None ->
         Lwt_log.debug "testing update_osds support" >>= fun () ->
         Lwt.catch
           (fun () ->
            do_it ()
            >>= fun () ->
            supports_update_osds := Some true;
            Lwt.return_unit
           )
           (let open Albamgr_protocol.Protocol.Error in
             function
             | Albamgr_exn (Unknown_operation,_) as exn ->
                Lwt_log.debug ~exn "no support; fake it" >>= fun () ->
                let () = supports_update_osds := Some false in
                fake_it()
             | exn -> Lwt.fail exn
           )
      | Some false -> fake_it()
      | Some true  -> do_it()

    method decommission_osd ~long_id =
      client # update DecommissionOsd long_id

    method mark_osd_claimed ~long_id =
      client # update MarkOsdClaimed long_id

    method mark_osd_claimed_by_other ~long_id ~alba_id =
      client # update MarkOsdClaimedByOther (long_id, alba_id)

    method add_work_items work_items =
      client # update
        AddWork
        (List.length work_items, work_items)

    method add_work_repair_fragment
      ~namespace_id ~object_id ~object_name
      ~chunk_id ~fragment_id ~version_id =
      self # add_work_items
           [ Work.RepairBadFragment (namespace_id,
                                     object_id,
                                     object_name,
                                     chunk_id,
                                     fragment_id,
                                     version_id) ]

    method get_work ~first ~finc ~last ~max ~reverse =
      client # query
        GetWork
        GetWorkParams.({ first; finc; last;
                         max; reverse; })

    method mark_work_completed ~work_id =
      client # update MarkWorkCompleted work_id

    method store_client_config ccfg =
      client # update StoreClientConfig ccfg

    method get_client_config =
      client # query
        ~consistency:Consistency.No_guarantees
        GetClientConfig ()

    method get_next_msgs : type dest msg.
      (dest, msg) Msg_log.t -> dest ->
      (Albamgr_protocol.Protocol.Msg_log.id * msg) counted_list_more Lwt.t =
      fun t dest ->
        client # query (GetNextMsgs t) dest

    method mark_msg_delivered : type dest msg.
      (dest, msg) Msg_log.t -> dest ->
      Albamgr_protocol.Protocol.Msg_log.id -> unit Lwt.t =
      fun t dest msg_id ->
        client # update (MarkMsgDelivered t) (dest, msg_id)


    method mark_msgs_delivered : type dest msg.
                                      (dest, msg) Msg_log.t -> dest ->
                                      Albamgr_protocol.Protocol.Msg_log.id ->
                                      Albamgr_protocol.Protocol.Msg_log.id -> unit Lwt.t =
      fun t dest from_msg_id to_msg_id ->
      let do_new () = client # update (MarkMsgsDelivered t) (dest, to_msg_id) in
      let do_old () =
        let rec inner from_msg_id =
          if from_msg_id > to_msg_id
          then Lwt.return ()
          else
            self # mark_msg_delivered t dest from_msg_id >>= fun () ->
            inner (Int32.succ from_msg_id)
        in
        inner from_msg_id
      in
      match !supports_mark_msgs_delivered with
      | None ->
         Lwt.catch
           (fun () ->
            do_new () >>= fun () ->
            supports_mark_msgs_delivered := Some true;
            Lwt.return_unit)
           (function
             | Error.Albamgr_exn (Error.Unknown_operation, _) ->
                supports_mark_msgs_delivered := Some false;
                do_old ()
             | exn -> Lwt.fail exn)
      | Some true ->
         do_new ()
      | Some false ->
         do_old ()

    method get_alba_id =
      client # query GetAlbaId ()

    method recover_namespace ~namespace ~nsm_host_id =
      client # update
        RecoverNamespace
        (namespace, nsm_host_id)

    method get_version =
      client # query GetVersion ()

    method statistics reset =
      client # query Statistics reset

    method check_can_claim ~long_id =
      client # query CheckClaimOsd long_id

    method list_osd_namespaces ~osd_id ~first ~finc ~last ~reverse ~max =
      client # query
        ListOsdNamespaces
        (osd_id,
         RangeQueryArgs.({
             first; finc; last;
             max; reverse;
           }))

    method list_all_osd_namespaces ~osd_id =
      list_all_x
        ~first:0l
        Std.id
        (self # list_osd_namespaces
           ~osd_id
           ~last:None
           ~max:(-1) ~reverse:false)

    method list_decommissioning_osds ~first ~finc ~last ~reverse ~max =
      client # query
        ListDecommissioningOsds
        RangeQueryArgs.({
            first; finc; last;
            max; reverse;
          })

    method list_all_decommissioning_osds =
      list_all_x
        ~first:0l
        fst
        (self # list_decommissioning_osds
           ~last:None
           ~max:(-1) ~reverse:false)

    method try_get_lease name counter =
      client # update
        TryGetLease
        (name, counter)

    method check_lease name =
      client # query
        CheckLease
        name

    method register_participant ~prefix ~name ~counter =
      client # update
        RegisterParticipant
        (prefix, (name, counter))

    method remove_participant ~prefix ~name ~counter =
      client # update
        RemoveParticipant
        (prefix, (name, counter))

    method get_participants ~prefix =
      client # query
        GetParticipants
        prefix

    method update_preset name updates =
      client # update
        UpdatePreset
        (name, updates)

    method get_progress name =
      client # query
        GetProgress
        name

    method get_progress_for_prefix name =
      client # query
        GetProgressForPrefix
        name

    method update_progress name old new_o =
      client # update
        UpdateProgress
        (name, Progress.Update.CAS (old, new_o))

    method get_maintenance_config =
      client # query
        GetMaintenanceConfig
        ()

    method update_maintenance_config u =
      client # update
        UpdateMaintenanceConfig
        u
  end

class single_connection_client (ic, oc) =
  let read_response deserializer =
    Llio.input_string ic >>= fun res_s ->
    let res_buf = Llio.make_buffer res_s 0 in
    match Llio.int_from res_buf with
    | 0 ->
      Lwt.return (deserializer res_buf)
    | ierr ->
      let err = Error.int2err ierr in
      Lwt_log.debug_f "albamgr operation failed: %i %s" ierr (Error.show err)
      >>= fun () ->
      let payload = Llio.string_from res_buf in
      Lwt.fail (Error.Albamgr_exn (err, payload))
  in
  let do_request tag serialize_request deserialize_response =
    let buf = Buffer.create 20 in
    Llio.int32_to buf tag;
    serialize_request buf;
    Lwt_log.debug_f "albamgr_client: %s" (tag_to_name tag) >>= fun () ->
    Lwt_unix.with_timeout
      10.
      (fun () ->
       Lwt_extra2.llio_output_and_flush oc (Buffer.contents buf) >>= fun () ->
       read_response deserialize_response
      )
  in
  object(self :# basic_client)
    method query : 'i 'o.
           ?consistency:Consistency.t ->
           ('i, 'o) Albamgr_protocol.Protocol.query -> 'i -> 'o Lwt.t =
      fun ?(consistency = Consistency.Consistent) command req ->
      do_request
        (command_to_tag (Wrap_q command))
        (fun buf ->
         Consistency.to_buffer buf consistency;
         write_query_i command buf req)
        (read_query_o command)

    method update : 'i 'o. ('i, 'o) Albamgr_protocol.Protocol.update -> 'i -> 'o Lwt.t =
      fun command req ->
      do_request
        (command_to_tag (Wrap_u command))
        (fun buf -> write_update_i command buf req)
        (read_update_o command)

    method do_unknown_operation =
      let code =
        Int32.add
          100l
          (List.map
             (fun (_, code, _) -> code)
             command_map
           |> List.max
           |> Option.get_some)
      in
      Lwt.catch
        (fun () ->
         do_request
           code
           (fun buf -> ())
           (fun buf -> ()) >>= fun () ->
         Lwt.fail_with "did not get an exception for unknown operation")
        (function
         | Error.Albamgr_exn (Error.Unknown_operation, _) -> Lwt.return ()
         | exn -> Lwt.fail exn)
  end

let wrap_around (ara_c:Arakoon_client.client) =
  ara_c # user_hook "albamgr" >>= fun (ic, oc) ->
  Llio.input_int32 ic
  >>= function
  | 0l -> begin
      Lwt_log.debug_f "user hook was found%!" >>= fun () ->
      let client = new single_connection_client (ic, oc) in
      Lwt.return client
    end
  | e ->
     let rc = Arakoon_exc.rc_of_int32 e in
     Llio.input_string ic >>= fun msg ->
     Lwt.fail (Arakoon_exc.Exception (rc, msg))

let wrap_around' ara_c =
  wrap_around ara_c >>= fun c ->
  Lwt.return (new client (c :> basic_client))

let make_client buffer_pool (ccfg:Arakoon_client_config.t) =
  let open Client_helper in
  find_master' ~tls:None ccfg >>= function
  | MasterLookupResult.Found (m , ncfg) ->
     let open Arakoon_client_config in
     Networking2.first_connection'
       buffer_pool
       ncfg.ips ncfg.port
       ~close_msg:"closing albamgr"
     >>= fun (fd, conn, closer) ->
     Lwt.catch
       (fun () ->
          Arakoon_remote_client.make_remote_client
            ccfg.cluster_id
            conn >>= fun client ->
          wrap_around (client:Arakoon_client.client))
       (fun exn ->
          closer () >>= fun () ->
          Lwt.fail exn)
     >>= fun c ->
     Lwt.return (c,
                 m,
                 closer)
  | r -> Lwt.fail (Client_helper.MasterLookupResult.Error r)

let _msg_of_exception = function
  | Protocol.Error.Albamgr_exn (err, _) ->
     Protocol.Error.show err
  | Client_helper.MasterLookupResult.Error err ->
     Client_helper.MasterLookupResult.to_string err
  | Arakoon_exc.Exception (rc, msg) ->
     Printf.sprintf "%s: %s" (Arakoon_exc.string_of_rc rc) msg
  | exn -> Printexc.to_string exn

let _with_client ~attempts cfg f =
  let attempt_it () =
    Lwt.catch
      (fun () ->
       Client_helper.with_master_client'
         ~tls:None
         (Albamgr_protocol.Protocol.Arakoon_config.to_arakoon_client_cfg cfg)
         (fun c -> wrap_around c >>= fun wc -> f wc)
       >>= fun r ->
       Lwt.return (`Success r)
      )
      (fun exn -> Lwt.return (`Failure exn))
  in
  let should_retry = function
    | Client_helper.MasterLookupResult.Error err -> true
    | _ -> false
  in
  let rec loop n d =
    attempt_it () >>= function
    | `Success r -> Lwt.return r
    | `Failure exn ->
       begin
         Lwt_log.debug_f "albamgr_client failed with %s" (_msg_of_exception exn) >>= fun () ->
         if n <= 1 || not (should_retry exn )
         then Lwt.fail exn
         else
           begin
             Lwt_log.debug_f "albamgr_client: n=%i sleep:%f before retry" n d >>= fun () ->
             Lwt_unix.sleep d >>= fun () ->
             loop (n-1) (d *. 2.0)
           end
       end
  in loop attempts 1.0

let with_client' ?(attempts=1) cfg f =
  _with_client ~attempts
    cfg
    (fun c -> f (new client (c :> basic_client)))
