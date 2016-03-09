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
open Lwt.Infix

let get_namespace_safety
      (alba_client : Alba_client.alba_client)
      ns_info dead_ns_osds =
  let open Albamgr_protocol.Protocol in
  let open Nsm_model in
  let namespace_id = ns_info.Namespace.id in

  Lwt_list.map_p
    (fun osd_id ->
       alba_client # osd_access # get_osd_info ~osd_id >>= fun (osd_info, _) ->
       Lwt.return (osd_id, osd_info.OsdInfo.node_id))
    dead_ns_osds
  >>= fun osds_with_node_id ->

  alba_client # nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->

  nsm # get_stats >>= fun { Nsm_model.NamespaceStats.bucket_count; } ->
  let res =
    List.filter
      (fun (policy, cnt) -> Int64.(cnt >: 0L))
      (snd bucket_count) |>
    List.map
      (fun (policy, cnt) ->
         let k, m, fragment_count, max_disks_per_node = policy in
         let applicable_dead_osds =
           Policy.get_applicable_osd_count
             max_disks_per_node
             osds_with_node_id
         in
         (policy, cnt, applicable_dead_osds, (fragment_count-k) - applicable_dead_osds)
      ) |>
    List.min
      ~compare:
        (fun
          (_, _, _, remaining_parity1) (_, _, _, remaining_parity2) ->
          compare remaining_parity1 remaining_parity2)
  in

  Lwt.return res


let get_disk_safety alba_client namespaces dead_osds =

  (* remove any duplicates *)
  let dead_osds = List.sort_uniq compare dead_osds in

  Lwt_list.map_p
    (fun osd_id ->
       alba_client # mgr_access # list_all_osd_namespaces ~osd_id
       >>= fun (_, osd_namespaces) ->
       let r =
         List.map
           (fun namespace_id -> (osd_id, namespace_id))
           osd_namespaces
       in
       Lwt.return r)
    dead_osds
  >>= fun osd_namespaces ->

  let dead_namespace_osds =
    List.group_by
      (fun (osd_id, namespace_id) -> namespace_id)
      (List.flatten_unordered osd_namespaces)
  in

  let get_dead_namespace_osds ~namespace_id =
    try List.map fst (Hashtbl.find dead_namespace_osds namespace_id)
    with Not_found -> []
  in

  Lwt_list.map_p
    (fun (namespace, ns_info) ->
       let open Albamgr_protocol.Protocol in
       let namespace_id = ns_info.Namespace.id in
       get_namespace_safety
         alba_client
         ns_info
         (get_dead_namespace_osds ~namespace_id) >>= fun r ->
       Lwt.return (namespace, r))
    namespaces
