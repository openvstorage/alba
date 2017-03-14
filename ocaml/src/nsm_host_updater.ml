(*
Copyright (C) 2017 iNuron NV

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

let check_nsm_host tls_config (ccfg:Arakoon_client_config.t) =
  let open Arakoon_client_config in
  let node_names = List.map
                     (fun (node_name, _)-> node_name)
                     ccfg.node_cfgs
  in
  let cluster_id = ccfg.cluster_id in
  let with_nsm_master ccfg tls_config f default =
    Lwt.catch
      (fun () ->
        Nsm_host_client.with_client
          (ccfg:Arakoon_client_config.t) tls_config f
      )
      (fun exn -> Lwt.return default)
  in
  with_nsm_master
    ccfg tls_config
    (fun c ->
      c # query Nsm_host_protocol.Protocol.GetHostVersion ()
    ) 0l
  >>= fun host_version ->
  Lwt_log.debug_f "host_version:%li" host_version >>= fun () ->
  if host_version = 1l
  then Lwt.return_true
  else
    begin
      Lwt_log.debug_f "checking %s" cluster_id >>= fun () ->
      let check_one node_name =
        Lwt.catch
          (fun () ->
            Lwt_log.debug_f "checking: node_name=%s" node_name
            >>= fun () ->
            Client_helper.with_client''
              ccfg node_name
              (fun c ->
                let consistency = Arakoon_client.No_guarantees in
                c # user_hook ~consistency "nsm_host" >>= fun (ic,oc) ->
                Llio.input_int32 ic >>= function
                | 0l -> begin
                    let open Nsm_protocol in
                    let open Nsm_host_protocol in
                    let session = Session.make () in
                    let scc = new Nsm_host_client.single_connection_client (ic,oc)
                                  session
                    in
                    scc # query ~consistency Protocol.GetVersion ()
                    >>= fun (major,minor,patch, commit) ->
                    Lwt_log.debug_f "version:(%i,%i,%i,%s)"
                                    major minor patch commit
                    >>= fun () ->
                    let update = Session.make_update 2 in
                    scc # query ~consistency Protocol.UpdateSession update
                    >>= fun processed ->
                    let () = Session.client_update session processed in
                    Lwt.return (Some session.Session.manifest_ser)
                  end
                | e ->
                   Lwt_log.warning_f "user hook was not found %li%!\n" e
                   >>= fun ()->
                   Lwt.return_none
              )
          )
          (fun exn ->
            Lwt.return_none)
      in

      Lwt_list.map_p check_one node_names >>= fun results ->
      Lwt_log.debug_f "results:%s" ([%show:int option list] results) >>= fun () ->
      let min_ver =
        List.fold_left
          (fun acc r ->
            match r with
            | None -> acc
            | Some v -> min acc v
          ) 2 results
      in

      begin
        if 2 = min_ver
        then
          Lwt_log.debug_f "updating host version" >>= fun () ->
          with_nsm_master
            ccfg tls_config
            (fun c ->
              c # update Nsm_host_protocol.Protocol.UpdateHostVersion (0l, 1l)
              >>= fun result_version ->
              Lwt_log.debug_f "resulting host version:%li" result_version
              >>= fun () ->
              Lwt.return_true
            ) false
        else
          Lwt.return_false
      end >>= fun r ->
      Lwt.return r
    end

let check tls_config (abm_ccfg: Arakoon_client_config.t) =
  Albamgr_client.with_client'
    ~attempts:1 abm_ccfg ~tls_config
    (fun abm ->
      abm # list_nsm_hosts
          ~first:""
          ~finc:true
          ~last:None
          ~max:(-1)
          ~reverse:false
      >>= fun ((_, hosts),_)  ->
      Lwt_list.map_p
        (fun (id, host,_) ->
          let open Albamgr_protocol.Protocol.Nsm_host in
          match host.kind with
          | Arakoon ccfg -> check_nsm_host tls_config ccfg
        ) hosts
      >>= fun results ->
      Lwt_log.debug_f "results:%s" ([%show: bool list] results) >>= fun () ->
      Lwt.return_unit



    )