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



let downloadable chunk_fragments =
  let rec build acc nones i = function
    | [] -> List.rev acc , nones
    | ((None, _), _, _) :: cfs -> build acc (nones+1) (i+1) cfs
    | ((Some osd, v), checksum, ctr) :: cfs ->
       let cl' = (osd, v), checksum, ctr in
       let acc' = (i,cl') :: acc
       and i' = i+1 in
       build acc' nones i' cfs
  in
  build [] 0 0 chunk_fragments

open Lwt.Infix

let sort_by_preference prefered_nodes osd_access
                       chunk_locations_i
  =
  let needed = Hashtbl.create 16 in
  let open Nsm_model in
  Lwt_list.iter_s
    (fun (_i,((osd_id, _version), _hash, _ctr)) ->
      osd_access # get_osd_info ~osd_id
      >>= fun (info,_,_) ->
      let node_id =  info.OsdInfo.node_id in
      let () = Hashtbl.add needed osd_id node_id in
      Lwt.return_unit
    )
    chunk_locations_i
  >>= fun () ->

  let cvalue (osd_id:osd_id) =
    let (node_id:OsdInfo.node_id)  = Hashtbl.find needed osd_id in
    if List.mem node_id prefered_nodes
    then 1
    else 0
  in
  let compare
        (_,((osd_id0, _),_,_))
        (_,((osd_id1, _),_,_))
    =
    let c0 = cvalue osd_id0
    and c1 = cvalue osd_id1
    in c1 - c0
  in
  let sorted = List.sort compare chunk_locations_i in
  Lwt.return sorted
