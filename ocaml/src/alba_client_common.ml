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
open Alba_client_errors

let get_best_policy policies osds_info_cache =
  let osds =
    Hashtbl.fold
      (fun osd_id osd_info acc ->
       (osd_id, osd_info.Nsm_model.OsdInfo.node_id) :: acc)
      osds_info_cache
      []
  in
  Policy.get_first_applicable_policy
    policies
    osds
  |> Option.map
       (fun (policy, cnt) ->
        policy, cnt, Policy.required_osds_per_node osds cnt)

let get_best_policy_exn policies osds_info_cache =
  match get_best_policy policies osds_info_cache with
  | None -> Error.(failwith NoSatisfiablePolicy)
  | Some p -> p

open Lwt.Infix

let find_prefered_osd prefered_nodes osd_access chunk_locations_i =
  let rec _inner = function
    | [] -> Lwt.return None
    | (_,((None,_),_)) :: rest -> _inner rest
    | (_,((Some osd_id, _),_)) as ci :: rest ->
       begin
         osd_access # get_osd_info ~osd_id
         >>= fun (info,_state,_caps) ->
         let node_id = info.Nsm_model.OsdInfo.node_id in
         if List.mem node_id prefered_nodes
         then
           begin
             Lwt_log.debug_f
               "clear preference for osd_id:%Li"
               osd_id >>= fun () ->
             Lwt.return (Some ci)
           end
         else
           _inner rest
       end
  in
  _inner chunk_locations_i
