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

open Alba_client_errors

let get_best_policy policies osds_info_cache =
  Policy.get_first_applicable_policy
    policies
    (Hashtbl.fold
       (fun osd_id osd_info acc ->
          (osd_id, osd_info.Albamgr_protocol.Protocol.Osd.node_id) :: acc)
       osds_info_cache
       [])

let get_best_policy_exn policies osds_info_cache =
  match get_best_policy policies osds_info_cache with
  | None -> Error.(failwith NoSatisfiablePolicy)
  | Some p -> p
