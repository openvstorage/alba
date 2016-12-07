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

let propagate_preset
      (mgr_access : Albamgr_client.client)
      (nsm_host_access : Nsm_host_access.nsm_host_access)
      ~preset_name =
  Lwt_log.info_f "Alba_client_preset_cache.propagate_preset %S" preset_name >>= fun () ->
  mgr_access # get_preset2 ~preset_name >>= function
  | None ->
     (* preset was deleted in the mean time *)
     Lwt.return ()
  | Some (_, preset, version, _, _) ->
     begin
       mgr_access # get_preset_propagation_state ~preset_name >>= function
       | None -> Lwt.return ()
       | Some (version', namespace_ids) ->
          Lwt_list.map_p
            (fun namespace_id ->
              Lwt.catch
                (fun () ->
                  nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
                  Lwt.return
                    (Some (namespace_id,
                           ns_info.Albamgr_protocol.Protocol.Namespace.nsm_host_id)))
                (let open Albamgr_protocol.Protocol.Error in
                 function
                 | Albamgr_exn (Namespace_does_not_exist, _) -> Lwt.return None
                 | exn -> Lwt.fail exn))
            namespace_ids
          >|= List.map_filter_rev Std.id
          >|= List.group_by snd
          >|= Hashtbl.to_assoc_list
          >>= Lwt_list.iter_p
                (fun (nsm_host_id, namespace_ids) ->
                  (nsm_host_access # get ~nsm_host_id)
                    # update_presets
                    (List.map
                       (fun (namespace_id, _) -> namespace_id, (preset, version))
                       namespace_ids) >>= fun results ->
                  List.iter
                    (function
                     | Ok _ -> ()
                     | Error err ->
                        let open Nsm_model.Err in
                        match err with
                        | Namespace_id_not_found -> () (* namespace was deleted in the mean time *)
                        | _ -> assert false)
                    results;
                  Lwt.return ())
          >>= fun () ->
          mgr_access # update_preset_propagation_state
                     ~preset_name
                     ~preset_version:version
                     ~namespace_ids
     end


class preset_cache
        (mgr_access : Albamgr_client.client)
        (nsm_host_access : Nsm_host_access.nsm_host_access) =
  let get_preset ~preset_name =
    mgr_access # get_preset ~preset_name >>= fun preset_o ->
    let _name, preset, _is_default, _in_use = Option.get_some preset_o in
    Lwt.return preset
  in
  let preset_cache = Hashtbl.create 3 in
  let get_preset_info ~preset_name =
    try Lwt.return (Hashtbl.find preset_cache preset_name)
    with Not_found ->
         get_preset ~preset_name >>= fun preset ->

         if not (Hashtbl.mem preset_cache preset_name)
         then begin
             Lwt.ignore_result begin
                 (* start a thread to refresh the preset periodically *)
                 let rec inner () =
                   Lwt.catch
                     (fun () ->
                      Lwt_extra2.sleep_approx 120. >>= fun () ->
                      get_preset ~preset_name >>= fun preset ->
                      Hashtbl.replace preset_cache preset_name preset;
                      Lwt.return ())
                     (fun exn ->
                      Lwt_log.debug_f
                        ~exn
                        "Error in refresh preset loop %S, ignoring"
                        preset_name) >>= fun () ->
                   inner ()
                 in
                 inner ()
               end;
             Hashtbl.replace preset_cache preset_name preset
           end;
         Lwt.return preset
  in
  object
    method get ~preset_name = get_preset_info ~preset_name
    method refresh ~preset_name =
      (* ensure a refresh thread is already running *)
      get_preset_info ~preset_name >>= fun _ ->
      get_preset ~preset_name >>= fun preset ->
      Hashtbl.replace preset_cache preset_name preset;

      propagate_preset
        mgr_access
        nsm_host_access
        ~preset_name
  end
