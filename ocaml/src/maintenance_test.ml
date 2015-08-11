(*
Copyright 2015 Open vStorage NV

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

open Lwt.Infix
open Prelude

let test_with_alba_client = Alba_test.test_with_alba_client

let with_nice_error_log f =
  let open Alba_client_errors in
  Lwt.catch
    f
    (function
      | (Error.Exn e)as x ->
         Lwt_log.info_f "failing: %s" (Error.show e)
         >>= fun () -> Lwt.fail x
      | x     -> Lwt.fail x
    )

let _wait_for_osds (alba_client:Alba_client.alba_client) namespace_id =
  let rec loop () =
    alba_client # nsm_host_access # refresh_namespace_osds ~namespace_id
    >>= fun (cnt, osd_ids) ->
    if cnt > 10
    then Lwt.return ()
    else Lwt_unix.sleep 0.1 >>= fun () -> loop ()
  in
  loop ()

let test_rebalance_one () =
  let test_name = "test_rebalance_one" in
  (*let preset_name = test_name in*)
  let namespace = test_name in
  let object_name = namespace in
  let open Nsm_model in
  test_with_alba_client
    (fun alba_client ->
     Lwt_log.debug "test_rebalance_one" >>= fun () ->
     alba_client # create_namespace ~namespace ~preset_name:None ()
     >>= fun namespace_id ->

     _wait_for_osds alba_client namespace_id >>= fun () ->

     let object_data =
       Lwt_bytes.of_string
         "Let's see if this test_rebalance_one thingy does its job"
     in
     alba_client # upload_object_from_bytes
       ~namespace
       ~object_name
       ~object_data
       ~checksum_o:None
       ~allow_overwrite:NoPrevious
     >>= fun (manifest,stats) ->
     Lwt_log.debug "uploaded object" >>= fun () ->

     let object_osds = Manifest.osds_used manifest.Manifest.fragment_locations in
     let set2s set= DeviceSet.elements set |> [%show : int32 list] in
     Lwt_log.debug_f "object_osds: %s" (set2s object_osds ) >>= fun () ->
     let get_targets () =
       alba_client
         # get_base_client
         # with_nsm_client ~namespace
         (fun nsm -> nsm # list_all_active_osds)
       >>= fun (n,osds_l) ->
       Lwt_log.debug_f "active_osds: %s" ([%show: int32 list] osds_l)
       >>= fun () ->
       let namespace_osds = DeviceSet.of_list osds_l in
       let targets = DeviceSet.diff namespace_osds object_osds in
       Lwt.return targets
     in
     get_targets () >>= fun targets ->

     let target_osd = DeviceSet.choose targets in
     let source_osd = DeviceSet.choose object_osds in
     let mc = new Maintenance.client (alba_client # get_base_client) in
     with_nice_error_log
       (fun () ->
        mc # rebalance_object
           ~namespace_id
           ~manifest
           ~source_osd
           ~target_osd
       )
     >>= fun object_locations_movements ->
     let source_osds =
       List.map
         (fun (c,f,s,t) -> s) object_locations_movements
     in
     alba_client # get_object_manifest'
       ~namespace_id ~object_name
       ~consistent_read:true ~should_cache:false
     >>= fun (_,mfo) ->
     begin
       match mfo with
       | None -> Lwt.fail_with "no more manifest?"
       | Some mf' ->
          Lwt_log.debug_f "mf':%s" (Manifest.show mf') >>= fun () ->
          let object_osds' = Manifest.osds_used mf'.Manifest.fragment_locations in
          let diff_from = DeviceSet.diff object_osds object_osds' in
          let diff_to   = DeviceSet.diff object_osds' object_osds in
          Lwt_log.debug_f "diff_from:%s" (set2s diff_from) >>= fun () ->
          Lwt_log.debug_f "diff_to  :%s" (set2s diff_to)   >>= fun () ->

          OUnit.assert_equal ~msg:"target_osd should match"
            ~printer:Int32.to_string
            (DeviceSet.choose diff_to) target_osd;

          List.iter
            (fun s ->
             OUnit.assert_bool
               "source_osd should be member"
               (DeviceSet.mem s diff_from)
            ) source_osds;

          Lwt.return ()
     end
    )


let _test_rebalance_namespace test_name fat ano categorize =
  let namespace = test_name in
  let preset_name = test_name in
  let object_name_template i = Printf.sprintf "object_name_%03i" i in
  test_with_alba_client
    (fun alba_client ->
     let open Albamgr_protocol.Protocol.Preset in
     alba_client # mgr_access # create_preset
       preset_name { _DEFAULT with policies = [(5,3,8,3)];}
     >>= fun () ->
     alba_client # create_namespace ~namespace ~preset_name:(Some preset_name) ()
     >>= fun namespace_id ->
     _wait_for_osds alba_client namespace_id >>= fun () ->

     let object_data =
         (String.init 16384 (fun i -> Char.chr ((i mod 26) + 65)))
     in
     Lwt_log.debug "uploading" >>= fun () ->
     let upload n =
       let rec _loop i =
         if i = n
         then Lwt.return ()
         else
           begin
             let object_name = object_name_template i in
             alba_client # get_base_client # upload_object_from_string
                         ~namespace
                         ~object_name
                         ~object_data
                         ~checksum_o:None
                         ~allow_overwrite:Nsm_model.NoPrevious
             >>= fun (manifest,stats) ->
             _loop (i+1)
           end
       in
       _loop 0
     in
     let n = 20 in
     with_nice_error_log (fun () -> upload 20) >>= fun () ->
     Lwt_log.debug_f "uploaded ... %i" n >>= fun () ->
     let mc = new Maintenance.client (alba_client # get_base_client) in
     let make_first () = "" in
     Lwt.catch
       (fun () ->
        mc # rebalance_namespace
           ~categorize
           ~make_first
           ~namespace_id
           ~only_once:true
           ()
       )
       (fun exn ->
        Lwt_log.debug_f ~exn "bad..." >>= fun () ->
        Lwt.fail exn
       )
     >>= fun () ->
     begin
       match !fat with
       | None -> Lwt.return ()
       | Some (fat_id,_) ->
          Rebalancing_helper.get_some_manifests
            (alba_client # get_base_client)
            ~make_first
            ~namespace_id
            fat_id
          >>= fun (n0,mfs) ->
          Lwt_log.debug_f "fat after: %i" n0
          >>= fun ()->
          OUnit.assert_bool
            "osd should touch less objects"
            (n0 < n/2);
          Lwt.return ()
     end
     >>= fun () ->
     begin
       match !ano with
       | None -> Lwt.return ()
       | Some (ano_id,_) ->
          Rebalancing_helper.get_some_manifests
            (alba_client # get_base_client)
            ~make_first
            ~namespace_id
            ano_id
          >>= fun (n0, mfs) ->
          Lwt_log.debug_f "anorectic after: %i" n0
          >>= fun () ->
          OUnit.assert_bool
            "osd should touch more objects"
            (n0 > n/2);
          Lwt.return ()
     end
    )

let test_rebalance_namespace_1 () =
  let test_name = "test_rebalance_namespace_1" in
  let fat = ref None in
  let ano = ref None in
  let categorize (n,fr) =
    match fr with
    | x :: y :: rest ->
       let () = fat := Some y in
       let () = ano := Some x in
       (1,[x]), (n-2,rest), (1,[y])
    | _ -> failwith "not enough osds in the namespace"
  in
  _test_rebalance_namespace test_name fat ano categorize

let test_rebalance_namespace_2 () =
  let test_name = "test_rebalance_namespace_2" in
    let fat = ref None in
  let ano = ref None in
  let categorize (n,fr) =
    match fr with
    | x :: rest ->
       let () = ano := Some x in
       (1,[x]),(n-1,rest),(0,[])
    | _ -> failwith "not enough osds in the namespace"
  in
  _test_rebalance_namespace test_name fat ano categorize

let rec wait_until f =
  f () >>= function
  | true -> Lwt.return ()
  | false ->
    Lwt_unix.sleep 0.1 >>= fun () ->
    wait_until f

let wait_for_namespace_osds alba_client namespace_id cnt =
  alba_client # nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->
  alba_client # nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
  wait_until
    (fun () ->
       let open Albamgr_protocol.Protocol in
       alba_client # deliver_nsm_host_messages
         ~nsm_host_id:ns_info.Namespace.nsm_host_id >>= fun () ->
       nsm # list_all_active_osds >>= fun (cnt', _) ->
       Lwt.return (cnt' >= cnt)) >>= fun () ->
  alba_client # nsm_host_access # refresh_namespace_osds ~namespace_id >>= fun _ ->
  Lwt.return ()

let test_repair_orange () =
  test_with_alba_client
    (fun alba_client ->
       let open Nsm_model in

       (*
          - upload object with a fragment missing
            (kill a fragment if the object happens to be complete)
          - call repair_by_policy? ~once:true
          - check that object is now complete
       *)

       let test_name = "test_repair_orange" in
       let namespace = test_name in
       alba_client # create_namespace ~namespace ~preset_name:None ()
       >>= fun namespace_id ->

       wait_for_namespace_osds (alba_client # get_base_client) namespace_id 11 >>= fun () ->

       let maintenance_client = new Maintenance.client (alba_client # get_base_client) in

       let object_name = test_name in
       let object_data = test_name in
       alba_client # get_base_client # upload_object_from_string
         ~namespace
         ~object_name
         ~object_data
         ~allow_overwrite:NoPrevious
         ~checksum_o:(Alba_test.get_checksum_o object_data) >>= fun (mf, _) ->

       Alba_test.maybe_delete_fragment
         ~update_manifest:true
         alba_client namespace_id mf 0 0 >>= fun () ->

       alba_client # nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->
       nsm # list_objects_by_policy'
         ~first:((5,4,8,0),"") ~finc:true
         ~last:(Some (((5,4,9,0), ""), false))
         ~max:10 >>= fun ((cnt, _), _) ->
       assert (cnt = 1);

       maintenance_client # repair_by_policy_namespace ~namespace_id >>= fun () ->

       alba_client # get_object_manifest
         ~namespace ~object_name
         ~consistent_read:true
         ~should_cache:false >>= fun (_, mf_o) ->
       let mf' = Option.get_some mf_o in

       let osd_id_o, version = Layout.index mf'.Manifest.fragment_locations 0 0 in
       assert (version = 2);
       assert (osd_id_o <> None);

       Lwt.return ())


let test_repair_orange2 () =
  test_with_alba_client
    (fun alba_client ->
       let test_name = "test_repair_orange2" in
       let namespace = test_name in

       (*
          - object with a too wide policy
          - kill a fragment from it
          - repair -> should use all osds
            (that is repairing shouldn't fail because it can't be fully repaired)
       *)

       let preset_name = test_name in
       let preset =
         Albamgr_protocol.Protocol.Preset.({
             _DEFAULT with
             policies = [ (2,20,5,4); ];
           }) in
       alba_client # mgr_access # create_preset
         preset_name
         preset >>= fun () ->

       alba_client # create_namespace ~preset_name:(Some preset_name) ~namespace ()
       >>= fun namespace_id ->

       let object_name = test_name in
       let object_data = get_random_string 399 in
       alba_client # get_base_client # upload_object_from_string
         ~namespace
         ~object_name
         ~object_data
         ~allow_overwrite:Nsm_model.NoPrevious
         ~checksum_o:None >>= fun (mf, object_id) ->

       Alba_test.maybe_delete_fragment
         ~update_manifest:true
         alba_client namespace_id mf 0 0 >>= fun () ->

       let maintenance_client = new Maintenance.client (alba_client # get_base_client) in
       maintenance_client # repair_by_policy_namespace ~namespace_id >>= fun () ->

       alba_client # get_object_manifest
         ~namespace ~object_name
         ~consistent_read:true
         ~should_cache:false >>= fun (_, mf_o) ->
       let mf' = Option.get_some mf_o in

       let open Nsm_model in
       let osd_id_o, version = Layout.index mf'.Manifest.fragment_locations 0 0 in
       assert (version = 2);
       assert (osd_id_o <> None);

       Lwt.return ())


open OUnit

let suite = "maintenance_test" >:::[
    "test_rebalance_one" >:: test_rebalance_one;
    "test_rebalance_namespace_1" >:: test_rebalance_namespace_1;
    "test_rebalance_namespace_2" >:: test_rebalance_namespace_2;
    "test_repair_orange" >:: test_repair_orange;
    "test_repair_orange2" >:: test_repair_orange2;
]
