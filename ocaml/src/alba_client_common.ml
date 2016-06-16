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
