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

open Lwt.Infix
open Prelude

let test_with_alba_client = Alba_test.test_with_alba_client
let with_maintenance_client = Alba_test.with_maintenance_client
let _wait_for_osds = Alba_test._wait_for_osds

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

let test_rebalance_one () =
  let test_name = "test_rebalance_one" in
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
     >>= fun (manifest,_, stats,_) ->
     Lwt_log.debug_f "uploaded object:%s" ([% show : Manifest.t] manifest) >>= fun () ->
     let base_client = alba_client # get_base_client in
     base_client # get_namespace_osds_info_cache ~namespace_id >>= fun cache ->

     let object_osds = Manifest.osds_used manifest.Manifest.fragment_locations in
     let set2s set= DeviceSet.elements set |> [%show : int64 list] in
     Lwt_log.debug_f "object_osds: %s" (set2s object_osds ) >>= fun () ->
     let get_targets () =
       alba_client
         # get_base_client
         # with_nsm_client ~namespace
         (fun nsm -> nsm # list_all_active_osds)
       >>= fun (n,osds_l) ->
       Lwt_log.debug_f "active_osds: %s" ([%show: int64 list] osds_l)
       >>= fun () ->
       let namespace_osds = DeviceSet.of_list osds_l in
       let targets = DeviceSet.diff namespace_osds object_osds in
       Lwt.return targets
     in
     get_targets () >>= fun targets ->

     let target_osd = DeviceSet.choose targets in
     let source_osd =
       let possible_sources =
         let node_id_of osd_id =
           let open Nsm_model.OsdInfo in
           let info = Hashtbl.find cache osd_id in
           info.node_id
         in
         let node_id = node_id_of target_osd in
         DeviceSet.filter (fun osd_id -> node_id_of osd_id = node_id) object_osds
       in
       DeviceSet.choose possible_sources
     in

     with_nice_error_log
       (fun () ->
        with_maintenance_client
          alba_client
          (fun mc ->
           mc # rebalance_object
              ~namespace_id
              ~manifest
              ~source_osd
              ~target_osd
       ))
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
            ~printer:Int64.to_string
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
     let open Preset in
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
             >>= fun (manifest,_, stats,_) ->
             _loop (i+1)
           end
       in
       _loop 0
     in
     let n = 20 in
     with_nice_error_log (fun () -> upload 20) >>= fun () ->
     Lwt_log.debug_f "uploaded ... %i" n >>= fun () ->
     let make_first_last_reverse () = "", None, false in
     Lwt.catch
       (fun () ->
        with_maintenance_client
          alba_client
          (fun mc ->
           mc # rebalance_namespace
              ~categorize
              ~make_first_last_reverse
              ~namespace_id
              ~only_once:true
              ()
       ))
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
            ~make_first_last_reverse
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
            ~make_first_last_reverse
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

       wait_for_namespace_osds alba_client namespace_id 11 >>= fun () ->

       let object_name = test_name in
       let object_data = test_name in
       alba_client # get_base_client # upload_object_from_string
         ~namespace
         ~object_name
         ~object_data
         ~allow_overwrite:NoPrevious
         ~checksum_o:(Alba_test.get_checksum_o object_data) >>= fun (mf,_, _,_) ->

       Alba_test.maybe_delete_fragment
         ~update_manifest:true
         alba_client namespace_id mf 0 0 >>= fun () ->

       alba_client # nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->
       nsm # list_objects_by_policy' ~k:5 ~m:4 ~max:10 >>= fun ((cnt, _), _) ->
       assert (cnt = 1);

       with_maintenance_client
         alba_client
         (fun mc ->
          mc # repair_by_policy_namespace' ~namespace_id)
       >>= fun () ->

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
        * object with a too wide policy
        * kill a fragment from it
        * repair -> should use all osds
        *  (that is repairing shouldn't fail because it can't be fully repaired)
        * the regenerated fragment should be a data fragment, not one of the parity fragments
        *)

       let preset_name = test_name in
       let preset =
         Preset.({
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
         ~checksum_o:None >>= fun (mf,_, object_id,_) ->

       Alba_test.maybe_delete_fragment
         ~update_manifest:true
         alba_client namespace_id mf 0 0 >>= fun () ->

       with_maintenance_client
         alba_client
         (fun mc ->
          mc # repair_by_policy_namespace ~namespace_id)
       >>= fun () ->

       alba_client # get_object_manifest
         ~namespace ~object_name
         ~consistent_read:true
         ~should_cache:false >>= fun (_, mf_o) ->
       let mf' = Option.get_some mf_o in

       Lwt_log.debug_f "new manifest: %s" (Nsm_model.Manifest.show mf') >>= fun () ->

       let open Nsm_model in
       (* missing data fragment is regenerated *)
       let osd_id_o, version = Layout.index mf'.Manifest.fragment_locations 0 0 in
       assert (version = 2);
       assert (osd_id_o <> None);

       Lwt.return ())

let test_rebalance_node_spread () =
  (* policy (5,4,8,4)
   * regular upload
   * move a fragment so all osds of a node are used
   * query buckets, assert
   * repair_by_policy
   * query buckets, assert
   *)
  let test_name = "test_rebalance_node_spread" in
  let namespace = test_name in
  test_with_alba_client
    (fun alba_client ->
     let preset_name = test_name in
     let preset =
       Preset.(
         { _DEFAULT with
           policies = [ (5,4,8,4); ];
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
                 ~checksum_o:None >>= fun (mf,_, _,_) ->

     let get_buckets () =
       alba_client # nsm_host_access
                   # with_nsm_client ~namespace
                   (fun nsm -> nsm # get_stats) >>= fun stats ->
       Lwt.return (snd stats.Nsm_model.NamespaceStats.bucket_count)
     in

     get_buckets () >>= fun r ->
     assert (r = [ (5,4,9,3), 1L; ]);

     (* move a fragment to create a sub awesome node spread *)
     let is_osd_used mf osd_id =
       List.mem (Some osd_id, 0) (List.hd_exn mf.Nsm_model.Manifest.fragment_locations)
     in
     let target_osd =
       let rec inner = function
         | [] -> assert false
         | osd_id :: tl ->
            if is_osd_used mf osd_id
            then inner tl
            else osd_id
       in
       inner [ 0L; 1L; 2L; 3L; ]
     in
     let source_osd =
       if is_osd_used mf 4L
       then 4L
       else 5L
     in
     with_maintenance_client
       alba_client
       (fun mc ->
        mc # rebalance_object
           ~namespace_id
           ~manifest:mf
           ~source_osd
           ~target_osd) >>= fun _ ->

     get_buckets () >>= fun r ->
     assert (r = [ (5,4,9,4), 1L; ]);

     with_maintenance_client
         alba_client
         (fun mc ->
          mc # repair_by_policy_namespace ~namespace_id)
       >>= fun () ->

     get_buckets () >>= fun r ->
     assert (r = [ (5,4,9,3), 1L; ]);

     Lwt.return ())

let test_rewrite_namespace () =
  let test_name = "test_rewrite_namespace" in
  let namespace = test_name in
  test_with_alba_client
    (fun alba_client ->
     alba_client # create_namespace
                 ~preset_name:None
                 ~namespace () >>= fun namespace_id ->

     let objs = ["1"; "2"; "a"; "b"] in

     let open Nsm_model in

     Lwt_list.map_p
       (fun name ->
        alba_client # get_base_client # upload_object_from_string
               ~namespace
               ~object_name:name
               ~object_data:name
               ~checksum_o:None
               ~allow_overwrite:NoPrevious >>= fun (mf,_, _,_) ->
        Lwt.return mf
       )
       objs >>= fun manifests ->

     let object_ids =
       List.map
         (fun mf -> mf.Manifest.object_id)
         manifests
     in

     let get_objs () =
       alba_client # with_nsm_client'
                   ~namespace_id
                   (fun client ->
                    client # list_objects_by_id
                           ~first:"" ~finc:true
                           ~last:None
                           ~max:100 ~reverse:false) >>= fun ((_, objs'), _) ->
       List.sort
         (fun mf1 mf2 ->
          let open Manifest in
          compare mf1.name mf2.name)
         objs'
       |> Lwt.return
     in

     get_objs () >>= fun manifests' ->
     assert (manifests' = manifests);

     let open Albamgr_protocol.Protocol in
     let name = "name" in
     let cnt = 10 in
     alba_client # mgr_access # add_work_items
                 [ Work.(IterNamespace
                           (Rewrite,
                            namespace_id,
                            name,
                            cnt)) ] >>= fun () ->

     Alba_test.wait_for_work alba_client >>= fun () ->
     Alba_test.wait_for_work alba_client >>= fun () ->

     alba_client # mgr_access # get_progress_for_prefix name >>= fun (cnt', progresses) ->
     assert (cnt = cnt');
     Lwt_log.debug_f "progresses:%s" ([%show : (int * Progress.t) list] progresses)
     >>= fun () ->
     let cnt'' =
       List.fold_left
         (fun acc (i, p) ->
          let end_key = get_start_key (i+1) cnt in
          match p with
          | Progress.Rewrite { Progress.count; next; } ->
             assert (end_key = next);
             acc + (Int64.to_int count)
          | _ -> assert false)
         0
         progresses
     in
     get_objs () >>= fun manifests' ->
     Lwt_log.debug_f "number of rewrites: %i" cnt'' >>= fun () ->
     OUnit2.assert_bool "not enough rewrites" (cnt'' >= (List.length objs));
     let obj_names' =
       List.fold_left
         (fun acc mf -> StringSet.add mf.Manifest.name acc)
         StringSet.empty
         manifests'
     in
     OUnit2.assert_equal ~msg:"not all objects were rewitten"
                         (StringSet.cardinal obj_names') (List.length objs);
     List.iter
       (fun mf -> assert (not (List.mem mf.Manifest.object_id object_ids)))
       manifests';

     Lwt.return ())

let test_verify_namespace () =
  let test_name = "test_verify_namespace" in
  test_with_alba_client
    (fun alba_client ->

     let preset_name = test_name in
     let preset' = Preset.({ _DEFAULT with policies = [ (5,4,8,3); ]; }) in
     alba_client # mgr_access # create_preset preset_name preset' >>= fun () ->

     let namespace = test_name in
     alba_client # create_namespace
                 ~preset_name:(Some preset_name)
                 ~namespace () >>= fun namespace_id ->

     let open Nsm_model in

     let object_name = "abc" in
     alba_client # get_base_client # upload_object_from_string
                 ~namespace
                 ~object_name
                 ~object_data:"efg"
                 ~checksum_o:None
                 ~allow_overwrite:NoPrevious >>= fun (mf,_, _,_) ->

     let object_id = mf.Manifest.object_id in

     (* remove a fragment *)
     let locations = List.hd_exn (mf.Manifest.fragment_locations) in
     let victim_osd_o, version0 = List.hd_exn locations in
     let victim_osd = Option.get_some victim_osd_o in
     Alba_test.delete_fragment
       alba_client namespace_id object_id
       (victim_osd, 0)
       0 0
     >>= fun () ->

     (* overwrite a fragment with garbage (to create a checksum mismatch) *)
     begin
       let osd_id_o, version_id = List.nth_exn locations 1 in
       let osd_id = Option.get_some osd_id_o in
       alba_client # with_osd
         ~osd_id
         (fun osd ->
          (osd # namespace_kvs namespace_id) # apply_sequence
              Osd.High
              []
              [ Osd.Update.set_string
                  (Osd_keys.AlbaInstance.fragment
                     ~object_id ~version_id
                     ~chunk_id:0
                     ~fragment_id:1)
                  (get_random_string 39)
                  Checksum.NoChecksum
                  false; ] >>= function
          | Osd.Ok -> Lwt.return ()
          | _ -> assert false)
     end >>= fun () ->

     let open Albamgr_protocol.Protocol in
     let name = "name" in
     let cnt = 10 in
     alba_client # mgr_access # add_work_items
                 [ Work.(IterNamespace
                           (Verify
                              { checksum = true;
                                repair_osd_unavailable = true; },
                            namespace_id,
                            name,
                            cnt)) ] >>= fun () ->

     Alba_test.wait_for_work alba_client >>= fun () ->
     Alba_test.wait_for_work alba_client >>= fun () ->

     alba_client # with_nsm_client'
                 ~namespace_id
                 (fun client ->
                  client # get_object_manifest_by_name object_name)
     >>= fun mfo ->
     let mf = Option.get_some mfo in
     assert (mf.Manifest.version_id = 1);

     (* missing fragment *)
     assert (mf.Manifest.fragment_locations
             |> List.hd_exn
             |> List.hd_exn
             |> snd
             = 1);

     (* checksum mismatch fragment *)
     assert (mf.Manifest.fragment_locations
             |> List.hd_exn
             |> fun l -> List.nth_exn l 1
             |> snd
             = 1);

     alba_client # mgr_access # get_progress_for_prefix name >>= fun (cnt', progresses) ->
     assert (cnt = cnt');
     let objects_verified,
         fragments_detected_missing,
         fragments_osd_unavailable,
         fragments_checksum_mismatch
       =
       List.fold_left
         (fun (objects_verified,
               fragments_detected_missing',
               fragments_osd_unavailable',
               fragments_checksum_mismatch')
              (i, p) ->
          let end_key = get_start_key (i+1) cnt in
          match p with
          | Progress.Verify ({ Progress.count; next; },
                             { Progress.fragments_detected_missing;
                               fragments_osd_unavailable;
                               fragments_checksum_mismatch })->
             assert (end_key = next);
             objects_verified + Int64.to_int count,
             fragments_detected_missing' + Int64.to_int fragments_detected_missing,
             fragments_osd_unavailable' + Int64.to_int fragments_osd_unavailable,
             fragments_checksum_mismatch' + Int64.to_int fragments_checksum_mismatch
          | _ -> assert false)
         (0, 0, 0, 0)
         progresses
     in
     assert (objects_verified = 1);
     assert (fragments_detected_missing = 1);
     assert (fragments_osd_unavailable = 0);
     assert (fragments_checksum_mismatch = 1);

     Lwt.return ())

let test_automatic_repair () =
  let test_name = "test_automatic_repair" in
  let namespace = test_name in
  test_with_alba_client
    (fun alba_client ->
    with_maintenance_client
      alba_client
      (fun maintenance_client ->

     let get_osd_has_objects ~osd_id =
       alba_client # get_base_client # with_nsm_client
                   ~namespace
                   (fun nsm ->
                    nsm # list_device_objects ~osd_id
                        ~first:"" ~finc:true
                        ~last:None ~max:1 ~reverse:false)
       >>= fun ((cnt, _), _) ->
       Lwt_log.debug_f "found %i objects on osd %Li" cnt osd_id >>= fun () ->
       Lwt.return (cnt > 0)
     in

     let port = 8980 in
     Asd_test.with_asd_client
       test_name port
       (fun asd ->
        alba_client # osd_access # seen
                    ~check_claimed:(fun _ -> true)
                    ~check_claimed_delay:1.
                    Discovery.(Good("",
                                    { id = test_name;
                                      extras =
                                        Some({ node_id = "bla";
                                               version = "AsdV1";
                                               total = 1L;
                                               used = 1L;
                                             });
                                      ips = ["127.0.0.1"];
                                      port = Some port;
                                      tlsPort = None;
                                      useRdma = false;
                                    })) >>= fun () ->
        alba_client # claim_osd ~long_id:test_name >>= fun osd_id ->

        alba_client # create_namespace
                    ~namespace
                    ~preset_name:None () >>= fun namespace_id ->

        wait_for_namespace_osds alba_client namespace_id 13 >>= fun () ->

        Lwt_list.iter_p
          (fun i ->
           alba_client # get_base_client # upload_object_from_string
                       ~namespace
                       ~object_name:(string_of_int i)
                       ~object_data:"fsdioap"
                       ~checksum_o:None
                       ~allow_overwrite:Nsm_model.NoPrevious
           >>= fun (mf,_, _,_) ->
           Lwt.return ())
          (Int.range 0 5) >>= fun () ->

        get_osd_has_objects ~osd_id >>= fun has_objects ->
        Lwt_log.debug_f "has_objects=%b" has_objects >>= fun () ->
        assert has_objects;
        Lwt.return osd_id) >>= fun osd_id ->

     Lwt_log.debug "cucu0" >>= fun () ->

     alba_client # mgr_access # update_maintenance_config
                 Maintenance_config.Update.(
       { enable_auto_repair' = Some true;
         auto_repair_timeout_seconds' = Some 10.;
         auto_repair_add_disabled_nodes = [];
         auto_repair_remove_disabled_nodes = [];
         enable_rebalance' = None;
         add_cache_eviction_prefix_preset_pairs = [];
         remove_cache_eviction_prefix_preset_pairs = [];
         redis_lru_cache_eviction' = None;
       }) >>= fun _ ->

     Lwt_log.debug "cucu2" >>= fun () ->

     Lwt.async
       (fun () ->
        Lwt.join
          [ maintenance_client # refresh_maintenance_config;
            alba_client # osd_access # propagate_osd_info ~delay:2. ();
            (Lwt_unix.sleep 1. >>= fun () ->
             Lwt.join
               [ maintenance_client # failure_detect_all_osds;
                 maintenance_client # repair_osds; ]); ]);

     Lwt_log.debug_f "cucu1" >>= fun () ->

     let rec wait_until_detected () =
       maintenance_client # should_repair ~osd_id >>= function
       | true -> Lwt.return ()
       | false ->
          Lwt_log.debug_f "wait some more" >>= fun () ->
          Lwt_unix.sleep 1. >>= fun () ->

          wait_until_detected ()
     in
     Lwt_unix.with_timeout 30. wait_until_detected >>= fun () ->

     Lwt_log.debug_f "cucu2" >>= fun () ->

     let rec wait_until_repaired () =
       get_osd_has_objects ~osd_id >>= function
       | true ->
          Lwt_unix.sleep 1. >>= fun () ->
          wait_until_repaired ()
       | false -> Lwt.return ()
     in
     Lwt_unix.with_timeout 90. wait_until_repaired
    ))


open OUnit

let suite = "maintenance_test" >:::[
    "test_rebalance_one" >:: test_rebalance_one;
    "test_rebalance_namespace_1" >:: test_rebalance_namespace_1;
    "test_rebalance_namespace_2" >:: test_rebalance_namespace_2;
    "test_repair_orange" >:: test_repair_orange;
    "test_repair_orange2" >:: test_repair_orange2;
    "test_rebalance_node_spread" >:: test_rebalance_node_spread;
    "test_rewrite_namespace" >:: test_rewrite_namespace;
    "test_verify_namespace" >:: test_verify_namespace;
    "test_automatic_repair" >:: test_automatic_repair;
]
