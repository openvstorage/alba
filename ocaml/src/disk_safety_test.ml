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

let test_safety () =
  let test_name = "test_safety" in
  Alba_test.test_with_alba_client
    (fun client ->
       let namespace = test_name in
       let preset_name = test_name in

       let open Albamgr_protocol.Protocol in
       let open Nsm_model in

       client # mgr_access # create_preset
         preset_name
         Preset.{ _DEFAULT
                  with
                    policies = [ (5,3,8,3); ]; } >>= fun () ->

       client # create_namespace
         ~preset_name:(Some preset_name)
         ~namespace () >>= fun namespace_id ->

       client # mgr_access # get_namespace ~namespace >>= fun ns_info_o ->
       let ns = Option.get_some ns_info_o in

       Disk_safety.get_disk_safety client [ ns; ] [] >>= fun res ->

       assert (snd (List.hd_exn res) |> List.hd = None);

       let object_name = test_name in

       client # get_base_client # upload_object_from_string
         ~epilogue_delay:None
         ~namespace
         ~object_name
         ~object_data:"y"
         ~checksum_o:None
         ~allow_overwrite:Nsm_model.NoPrevious >>= fun (mf,_, _,_) ->
       let object_id = mf.Manifest.object_id in

       let assert_some_res res bucket' cnt' applicable_dead_osds' remaining_safety' =
         let { Disk_safety.bucket; count; applicable_dead_osds; remaining_safety } =
           Option.get_some (snd (List.hd_exn res) |> List.hd)
         in
         OUnit.assert_equal ~msg:"policy" bucket bucket' ~printer:Policy.show_policy;
         OUnit.assert_equal ~msg:"cnt" count  cnt';
         OUnit.assert_equal ~msg:"applicable_dead_osds" applicable_dead_osds applicable_dead_osds';
         OUnit.assert_equal ~msg:"remaining_safety" remaining_safety remaining_safety';
       in

       Disk_safety.get_disk_safety client [ ns; ] [] >>= fun res ->
       assert_some_res res (5,3,8,3) 1L 0 3;

       Disk_safety.get_disk_safety client [ ns; ] [ 0L; ] >>= fun res ->
       assert_some_res res (5,3,8,3) 1L 1 2;

       (* get all osds belonging to node 2 *)
       begin
         client # mgr_access # list_all_osds >>= fun (_, osds) ->
         let osd_ids =
           List.filter
             (function
               | (Osd.ClaimInfo.ThisAlba id, osd_info) ->
                 let node_id = osd_info.OsdInfo.node_id in
                 if String.length node_id > 5
                 then "_2" = Str.last_chars node_id 2
                 else false
               | _ -> false)
             osds |>
           List.map
             (function
               | (Osd.ClaimInfo.ThisAlba id, _) -> id
               | _ -> failwith "can't happen")
         in
         OUnit.assert_equal ~msg:"osd_ids?" 4 (List.length osd_ids) ~printer:string_of_int;
         Lwt.return osd_ids
       end >>= fun osd_ids ->

       Disk_safety.get_disk_safety client [ ns; ] osd_ids >>= fun res ->
       assert_some_res res (5,3,8,3) 1L 3 0;

       (* mark a fragment as lost, then assert disk safety again... *)
       begin
         client # nsm_host_access # get_gc_epoch ~namespace_id
         >>= fun gc_epoch ->
         let version_id = mf.Manifest.version_id + 1 in
         client # with_nsm_client'
           ~namespace_id
           (fun client ->
              client # update_manifest
                ~object_name
                ~object_id
                [ (0, 0, None, None) ]
                ~gc_epoch ~version_id
           )
       end >>= fun () ->

       Disk_safety.get_disk_safety client [ ns; ] [] >>= fun res ->
       (* disk safety is reduced by one *)
       assert_some_res res (5,3,7,3) 1L 0 2;

       Lwt.return ())

open OUnit

let suite = "disk_safety_test" >:::[
    "test_safety" >:: test_safety
  ]
