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
open Slice
open Recovery_info
open Lwt.Infix
open Lwt_bytes2


let gc_grace_period = Nsm_host_access.gc_grace_period
type namespace_id = Albamgr_protocol.Protocol.Namespace.id

exception NotMyTask



module MStats = struct
  let section = Lwt_log.Section.make "mstats"

  type tag =
    | REBALANCE
    | REPAIR_GENERIC_TIME
    | REPAIR_GENERIC_COUNT
    | REWRITE_OBJECT
    | CLEAN_OBSOLETE

  let int32_of, name_of, name_of_i32 =
    let mapping = [
      (REBALANCE,0l, "Rebalance");
      (REPAIR_GENERIC_TIME,1l,"Repair_generic_time");
      (REPAIR_GENERIC_COUNT,2l,"Repair_generic_count");
      (REWRITE_OBJECT,3l, "Rewrite_object");
      (CLEAN_OBSOLETE, 4l, "Clean_obsolete");
    ]
    in
    let i32 = Hashtbl.create 16 in
    let nms = Hashtbl.create 16 in
    let ton = Hashtbl.create 16 in
    List.iter
      (fun (t,i,n) ->
       Hashtbl.add i32 t i;
       Hashtbl.add nms t n;
       Hashtbl.add ton i n;
      ) mapping;
    Hashtbl.find i32,
    Hashtbl.find nms,
    Hashtbl.find ton

  open Statistics_collection
  let stats = ref (Generic.make ())

  let new_delta tag delta =
    let i32 = int32_of tag in
    Generic.new_delta !stats i32 delta

  let show_stats () =
    Generic.show_inner !stats name_of_i32

  let stop () = Generic.stop !stats
end


class client ?(retry_timeout = 60.)
             ?(load = 1)
             (alba_client : Alba_base_client.client)
  =
  let tls_config = alba_client # tls_config in
  let mgr_access = alba_client # mgr_access in
  let nsm_host_access = alba_client # nsm_host_access in
  let coordinator =
    let open Maintenance_coordination in
    make_maintenance_coordinator
      mgr_access
      ~lease_name:maintenance_lease_name
      ~lease_timeout:maintenance_lease_timeout
      ~registration_prefix:maintenance_registration_prefix
  in
  let () = coordinator # init in
  let filter item_id =
    (* item_id could be e.g. namespace_id or work item id *)
    let remainder =
      Int64.rem item_id (coordinator # get_modulo |> Int64.of_int)
      |> Int64.to_int
    in remainder = coordinator # get_remainder
  in

  let osd_access = alba_client # osd_access in

  let throttle_pool =
      let max_size = load in
      Lwt_pool2.create max_size
                       ~check:(fun _ _ -> false)
                       ~factory:(fun () -> Lwt.return_unit)
                       ~cleanup:(fun () -> Lwt.return_unit)
  in
  let with_throttling f =
    Lwt_pool2.use throttle_pool f
  in
  let _throttled_repair_object_generic_and_update_manifest
        alba_client
         ~namespace_id
         ~manifest
         ~problem_fragments
         ~problem_osds
    =
    with_throttling
      (fun () ->
       Repair.repair_object_generic_and_update_manifest
         alba_client
         ~namespace_id
         ~manifest
         ~problem_fragments
         ~problem_osds
      )
  in

  let _timed_repair_object_generic_and_update_manifest
        ~namespace_id ~manifest ~problem_fragments ~problem_osds
    =
    Prelude.with_timing_lwt
      (fun () ->
       _throttled_repair_object_generic_and_update_manifest
         alba_client
         ~namespace_id
         ~manifest
         ~problem_fragments
         ~problem_osds
      )
    >>= fun (delta,()) ->
    MStats.new_delta MStats.REPAIR_GENERIC_TIME delta;
    MStats.new_delta MStats.REPAIR_GENERIC_COUNT (Int64Set.cardinal problem_osds |> float);
    Lwt.return_unit
  in
  let _throttled_rewrite_object alba_client ~namespace_id ~manifest =
    with_throttling
      (fun () ->
       Repair.rewrite_object
         alba_client
         ~namespace_id
         ~manifest
      )
  in
  let _timed_rewrite_object alba_Client ~namespace_id ~manifest =
    Prelude.with_timing_lwt
      (fun () ->
       _throttled_rewrite_object
         alba_client
         ~namespace_id ~manifest
      )
    >>= fun (delta,()) ->
    MStats.new_delta MStats.REWRITE_OBJECT delta;
    Lwt.return_unit

  in
  object(self)

    method get_coordinator = coordinator

    val mutable maintenance_config =
      Maintenance_config.({ enable_auto_repair = false;
                            auto_repair_timeout_seconds = 1.;
                            auto_repair_disabled_nodes = [];
                            enable_rebalance = false;
                            cache_eviction_prefix_preset_pairs = Hashtbl.create 0;
                            redis_lru_cache_eviction = None;
                          })

    method get_maintenance_config = maintenance_config

    method refresh_maintenance_config : unit Lwt.t =
      Lwt_extra2.run_forever
        "refresh_maintenance_config"
        (fun () ->
         mgr_access # get_maintenance_config >>= fun cfg ->
         maintenance_config <- cfg;
         Lwt.return_unit)
        retry_timeout

    val purging_osds = Hashtbl.create 3
    method refresh_purging_osds ?(once = false) () : unit Lwt.t =
      let inner () =
        mgr_access # list_all_purging_osds >>= fun (_, purging_osds') ->
        List.iter
          (fun osd_id -> Hashtbl.add purging_osds osd_id ())
          purging_osds';
        Lwt.return ()
      in

      if once
      then inner ()
      else
        Lwt_extra2.run_forever
          "refresh_purging_osds"
          inner
          retry_timeout

    val maybe_dead_osds = Hashtbl.create 3
    method should_repair ~osd_id =
      osd_access # get_osd_info ~osd_id >>= fun (osd_info, _, _) ->
      if maintenance_config.Maintenance_config.enable_auto_repair
         && Hashtbl.mem maybe_dead_osds osd_id
      then
        Lwt_log.info_f "%Li may be dead => should repair" osd_id
        >>= fun () ->
        Lwt.return true
      else
        if osd_info.Nsm_model.OsdInfo.decommissioned
        then
          Lwt_log.info_f "%Li was decommissioned => should repair" osd_id
          >>= fun () ->
          Lwt.return true
        else
          Lwt.return false

    method failure_detect_all_osds : unit Lwt.t =
      let first_time = ref true in
      let task_name = "failure_detect_all_osds" in
      Lwt_extra2.run_forever
        task_name
        (fun () ->
         Lwt_log.info task_name >>= fun () ->
         if maintenance_config.Maintenance_config.enable_auto_repair
         then
           begin
             mgr_access # list_all_claimed_osds >>= fun (_, osds) ->

             (* failure detecting already decommissioned osds isn't that useful *)
             let osds =
               List.filter
                 (fun (_, osd_info) -> not osd_info.Nsm_model.OsdInfo.decommissioned)
                 osds
             in
             Lwt_list.map_s
               (fun (osd_id, osd_info) ->
                 alba_client # osd_access # get_osd_info ~osd_id
                 >>= fun (_, osd_state, _) ->
                 Lwt.return (osd_id, osd_info, osd_state)
               ) osds
             >>= fun osds_with_state ->

             let load osds_with_state () =
               Automatic_repair.periodic_load_osds
                  alba_client
                  maintenance_config
                  osds_with_state
             in
             begin
               if !first_time
               then
                 load osds_with_state () >>= fun () ->
                 let () = first_time := false in
                 Lwt.return_unit
               else
                 let () = Lwt.async (load osds_with_state) in
                 Lwt.return_unit
             end >>= fun () ->
             let past_date =
               Unix.gettimeofday () -.
                 maintenance_config.Maintenance_config.auto_repair_timeout_seconds
             in
             List.iter
               (fun (osd_id,
                     (osd_info:Nsm_model.OsdInfo.t),
                     (osd_state:Osd_state.t)) ->

                 let open Nsm_model.OsdInfo in
                 let open Automatic_repair in
                 let open Osd_state in

                 let have_read =
                   recent_enough past_date osd_info.read
                   || recent_enough past_date osd_state.read
                 and have_write =
                   recent_enough past_date osd_info.write
                   || recent_enough past_date osd_state.read
                 in
                 let alive =
                   (List.mem osd_info.node_id maintenance_config.Maintenance_config.auto_repair_disabled_nodes)
                   || (have_read && have_write)
                 in
                 if alive
                 then Hashtbl.remove maybe_dead_osds osd_id
                 else Hashtbl.replace maybe_dead_osds osd_id ()
               )
               osds_with_state;

             Lwt.return ()
           end
         else
           Lwt.return ())
        retry_timeout

    method rebalance_object
             ~(namespace_id:int64)
             ~manifest
             ~(source_osd:Nsm_model.osd_id)
             ~(target_osd:Nsm_model.osd_id)
           : Nsm_model.FragmentUpdate.t list Lwt.t
      =
      let open Nsm_model in
      let open Manifest in
      let n_chunks = Manifest.n_chunks manifest in
      let osds_touched = Manifest.osds_used manifest in

      let () =
        if DeviceSet.mem target_osd osds_touched
        then
          let msg =
            Printf.sprintf
              "bad move: source:%Li => target:%Li touched:%s"
              source_osd target_osd
              ([%show : int64 list] (DeviceSet.elements osds_touched))
          in
          failwith msg
      in
      let object_id = manifest.object_id in
      let object_name = manifest.name in

      Lwt_log.debug_f
        "rebalance_object %S ~source_osd:%Li ~target_osd:%Li"
        object_name source_osd target_osd
      >>= fun () ->

      let version_id0 = manifest.version_id in
      let version_id1 = version_id0 + 1 in
      let fragment_info = Manifest.combined_fragment_infos manifest in
      alba_client # get_ns_preset_info ~namespace_id
      >>= fun preset ->

      nsm_host_access # get_gc_epoch ~namespace_id
      >>= fun gc_epoch ->

      let enc = manifest.encrypt_info in
      let encryption =
        Encrypt_info_helper.get_encryption preset enc
      in
      Lwt_list.map_s
        (fun (chunk_id, chunk_location) ->
         let source_fragment =
           let rec _find i = function
             | [] -> failwith "all fragments reside on target_osd"
             | f :: fs ->
                let ((osd_id_o, version), checksum, fragment_ctr) = f in
                if osd_id_o = Some source_osd
                then (i, checksum, version, fragment_ctr)
                else _find (i+1) fs
           in _find 0 chunk_location
         in
         let (fragment_id, fragment_checksum,version, fragment_ctr) = source_fragment in

         let object_info_o =
           let is_last_chunk = chunk_id = n_chunks - 1 in
           if is_last_chunk
           then
             let open RecoveryInfo in
             Some {
                 storage_scheme = manifest.storage_scheme;
                 size = manifest.size;
                 checksum = manifest.checksum;
                 timestamp = manifest.timestamp;
             }
           else None
         in


         RecoveryInfo.make
           ~object_name
           ~object_id
           object_info_o
           encryption
           (Manifest.chunk_size manifest chunk_id)
           (List.nth_exn manifest.fragments chunk_id
            |> List.map Fragment.len_of
           )
           (List.nth_exn manifest.fragments chunk_id
            |> List.map Fragment.crc_of
           )
           fragment_ctr
         >>= fun recovery_info_slice ->

         let open Alba_client_errors.Error in

         (* TODO get unpacked fragment from cache if available? *)
         let location = (source_osd, version) in
         Alba_client_download.download_packed_fragment
           osd_access
           ~location
           ~namespace_id
           ~object_id ~object_name
           ~chunk_id ~fragment_id
         >>= function
         | Prelude.Error.Error x ->
            Lwt.fail (Exn NotEnoughFragments)
         | Prelude.Error.Ok (_, packed_fragment) ->
            Lwt.finalize
              (fun () ->
               Fragment_helper.verify packed_fragment fragment_checksum
               >>= fun checksum_valid ->
               if not checksum_valid
               then Lwt.fail (Exn ChecksumMismatch)
               else
                 begin
                   Alba_client_upload.upload_packed_fragment_data
                     osd_access
                     ~namespace_id ~object_id
                     ~chunk_id ~fragment_id ~version_id:version_id1
                     ~packed_fragment
                     ~checksum:fragment_checksum
                     ~gc_epoch
                     ~recovery_info_blob:(Osd.Blob.Slice recovery_info_slice)
                     ~osd_id:target_osd
                   >>= fun apply_result' ->
                   let fu = FragmentUpdate.make
                              chunk_id fragment_id
                              (Some target_osd) None
                              fragment_ctr apply_result'
                   in
                   Lwt.return fu
                 end)
              (fun () ->
               Lwt_bytes.unsafe_destroy packed_fragment;
               Lwt.return_unit)
         )
        (List.mapi (fun i lc -> i, lc) fragment_info)
      >>= fun fragment_updates ->

      alba_client # with_nsm_client'
        ~namespace_id
        (fun client ->
         client # update_manifest
                ~object_name
                ~object_id
                fragment_updates
                ~gc_epoch ~version_id:version_id1
        )
      >>= fun () ->
      let size = List.length fragment_updates in
      let () = MStats.new_delta MStats.REBALANCE (float size) in
      Lwt.return fragment_updates


    method decommission_device
             ?(deterministic = false)
             ?(should_repair = ref true)
             ~namespace_id
             ~osd_id ()
      =
      if filter namespace_id
      then self # decommission_device'
                ~deterministic
                ~should_repair
                ~namespace_id
                ~osd_id ()
      else Lwt.return ()

    method decommission_device'
             ?(deterministic = false)
             ?(should_repair = ref true)
             ~namespace_id
             ~osd_id ()
      =
      Lwt_log.debug_f
        "Decommissioning osd %Li namespace_id:%Li"
        osd_id namespace_id
      >>= fun () ->
      let first, last, reverse =
        if deterministic
        then "", None, false
        else make_first_last_reverse ()
      in
      alba_client # with_nsm_client'
        ~namespace_id
        (fun client ->
           client # list_device_objects
             ~osd_id
             ~first ~finc:true ~last
             ~max:100 ~reverse)
      >>= fun ((cnt, manifests), has_more) ->
      let repaired_some = ref false in
      Lwt_log.debug_f
        "Decommissioning osd:%Li namespace_id:%Li first:%S ~reverse:%b cnt:%i, has_more:%b"
        osd_id namespace_id first reverse cnt has_more
      >>= fun () ->
      Lwt_list.iter_s
        (fun manifest ->
         if Hashtbl.mem purging_osds osd_id
         then
           nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->
           alba_client # with_nsm_client'
             ~namespace_id
             (fun client ->
              let open Nsm_model.Manifest in
              let _, updated_locations =
                List.fold_left
                  (fun (chunk_id, acc) fragments ->
                    let _, acc =
                      List.fold_left
                        (fun (fragment_id, acc) f  ->
                          let (osd_id_o, version) = Nsm_model.Fragment.loc_of f in
                          let acc = match osd_id_o with
                            | None -> acc
                            | Some osd_id ->
                               if Hashtbl.mem purging_osds osd_id
                               then
                                 (Nsm_model.FragmentUpdate.make
                                    chunk_id fragment_id
                                    None None None None
                                 ):: acc
                               else acc
                          in
                          (fragment_id + 1, acc))
                        (0, acc)
                        fragments
                    in
                    (chunk_id + 1, acc))
                  (0, [])
                  manifest.fragments
              in
              Lwt.catch
                (fun () ->
                 client # update_manifest
                        ~object_name:manifest.name
                        ~object_id:manifest.object_id
                        updated_locations
                        ~gc_epoch
                        ~version_id:(manifest.version_id + 1) >>= fun () ->
                 repaired_some := true;
                 Lwt.return_unit)
                (fun exn ->
                 let open Nsm_model.Manifest in
                 Lwt_log.info_f
                   ~exn
                   "Exn while purging osd %Li (~namespace_id:%Li ~object ~name:%S ~object_id:%S), will now try object rewrite"
                   osd_id namespace_id manifest.name manifest.object_id >>= fun () ->
                 Lwt_extra2.ignore_errors
                   ~logging:true
                   (fun () -> _timed_rewrite_object alba_client ~namespace_id ~manifest >>= fun () ->
                              repaired_some := true;
                              Lwt.return_unit))
             )
         else
           Lwt.catch
             (fun () ->
              _timed_repair_object_generic_and_update_manifest
                ~namespace_id
                ~manifest
                ~problem_fragments:[]
                ~problem_osds:(Int64Set.of_list [ osd_id ]) >>= fun () ->
              repaired_some := true;
              Lwt.return_unit
             )
             (fun exn ->
              let open Nsm_model.Manifest in
              Lwt_log.info_f
                ~exn
                "Exn while repairing osd %Li (~namespace_id:%Li ~object ~name:%S ~object_id:%S), will now try object rewrite"
                osd_id namespace_id manifest.name manifest.object_id >>= fun () ->
              Lwt_extra2.ignore_errors
                ~logging:true
                (fun () -> _timed_rewrite_object alba_client ~namespace_id ~manifest >>= fun () ->
                           repaired_some := true;
                           Lwt.return_unit)
             )
        )
        manifests >>= fun () ->

      if has_more && !should_repair && !repaired_some
      then self # decommission_device ~deterministic ~namespace_id ~osd_id ()
      else Lwt.return ()

    method repair_osd ~osd_id =
      let threads = Hashtbl.create 3 in
      let should_repair = ref true in
      let rec inner () =
        Lwt.catch
          (fun () ->
            mgr_access # list_all_osd_namespaces ~osd_id >>= fun (_, namespaces) ->

            List.iter
              (fun namespace_id ->
                if not (Hashtbl.mem threads namespace_id)
                then
                  begin
                    let t =
                      Lwt_extra2.ignore_errors
                        (fun () ->
                          self # decommission_device
                               ~should_repair
                               ~namespace_id
                               ~osd_id ()) >>= fun () ->
                      Hashtbl.remove threads namespace_id;
                      Lwt.return ()
                    in
                    Hashtbl.add threads namespace_id t;
                    Lwt.async (fun () -> t)
                  end)
              namespaces;

            Lwt.return ())
          (fun exn ->
            Lwt_log.info_f ~exn "Exception in repair_osd %Li" osd_id) >>= fun () ->
        Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
        self # should_repair ~osd_id >>= fun should_repair' ->
        if should_repair'
        then inner ()
        else
          begin
            (* wait for all threads to finish first
             * (so we never get into a situation where the same maintenance
             * process has multiple repair threads fighting each other)
             *)
            should_repair := false;
            let ts = Hashtbl.fold (fun _ t acc -> t :: acc) threads [] in
            Lwt.join ts
          end
      in
      inner ()

    method repair_osds : unit Lwt.t =
      let threads = Hashtbl.create 3 in
      let rec inner () =
        mgr_access # list_all_decommissioning_osds >>= fun (_, osds) ->

        let ensure_repair_thread osd_id =
          if not (Hashtbl.mem threads osd_id)
          then begin
              Hashtbl.add threads osd_id ();
              Lwt.async
                (fun () ->
                 Lwt_extra2.ignore_errors
                   (fun () -> self # repair_osd ~osd_id) >>= fun () ->
                 Hashtbl.remove threads osd_id;
                 Lwt.return ())
            end
        in

        List.iter
          (fun (osd_id, _) -> ensure_repair_thread osd_id)
          osds;

        if maintenance_config.Maintenance_config.enable_auto_repair
        then
          Hashtbl.iter
            (fun osd_id () -> ensure_repair_thread osd_id)
            maybe_dead_osds;

        Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
        inner ()
      in
      Lwt_extra2.run_forever
        "Unexpected exception in repair_osds"
        inner
        retry_timeout

    method clean_device_obsolete_keys ~namespace_id ~osd_id =
      alba_client # with_nsm_client'
        ~namespace_id
        (fun nsm_client ->
           nsm_client # list_device_keys_to_be_deleted
             ~osd_id
             ~first:"" ~finc:true ~last:None
             ~max:100 ~reverse:false)
      >>= fun ((_, keys_to_be_deleted), has_more) ->

      (* The if below isn't strictly needed, but it prevents us from sending
         empty update sequences to the asds, and then notifying back to
         the nsm that we successfully deleted 0 keys. *)
      if keys_to_be_deleted <> []
      then begin
        let upds =
          List.map
            Osd.Update.delete_string
            keys_to_be_deleted
        in

        Lwt_log.debug_f
          "Cleaning obsolete keys for osd_id=%Li %S"
          osd_id
          ([%show : string list] keys_to_be_deleted) >>= fun () ->
        Prelude.with_timing_lwt
          (fun () ->
           alba_client # with_osd
                       ~osd_id
                       (fun osd_client ->
                        (osd_client # namespace_kvs namespace_id) # apply_sequence
                                   Osd.Low
                                   [] upds >>= function
                        | Ok _    -> Lwt.return_unit
                        | Error x ->
                           Lwt_log.warning_f "%s: deletes should never fail"
                                             ([%show : Osd.Error.t] x)
                           >>= fun () ->
                           assert false;
                       )
           >>= fun () ->
           alba_client # with_nsm_client'
                       ~namespace_id
                       (fun nsm_client ->
                        nsm_client # mark_keys_deleted
                                   ~osd_id
                                   ~keys:keys_to_be_deleted)
          )
        >>= fun (delta,()) ->
        MStats.new_delta MStats.CLEAN_OBSOLETE delta;

        if has_more && filter namespace_id
        then self # clean_device_obsolete_keys ~namespace_id ~osd_id
        else Lwt.return ()
      end else
        Lwt.return ()


    method clean_obsolete_keys_namespace ?(once=false) ~namespace_id =
      if once
      then
        begin
          nsm_host_access # get_namespace_info ~namespace_id >>= fun (_, devices, _) ->

          Lwt_list.iter_p
            (fun osd_id ->
             with_throttling
               (fun () ->
                self # clean_device_obsolete_keys ~namespace_id ~osd_id)
            )
            devices
        end
      else
        begin
          let threads = Hashtbl.create 3 in
          let rec inner () =
            (if filter namespace_id
             then
               begin
                 nsm_host_access # get_namespace_info ~namespace_id >>= fun (_, osds, _) ->
                 List.iter
                   (fun osd_id ->
                    if not (Hashtbl.mem threads osd_id)
                    then begin
                        Hashtbl.add threads osd_id ();
                        Lwt.async
                          (fun () ->
                           Lwt_extra2.ignore_errors
                             (fun () ->
                              self # clean_device_obsolete_keys
                                   ~namespace_id ~osd_id) >>= fun () ->
                           Hashtbl.remove threads osd_id;
                           Lwt.return ())
                      end)
                   osds;
                 Lwt.return_unit
               end
             else
               Lwt.return_unit) >>= fun () ->

            Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
            inner ()
          in
          inner ()
        end

    method garbage_collect_device
             ~osd_id
             ~namespace_id
             ~gc_epoch
      =
      let open Osd_keys in
      let open Slice in
      let last =
        let l = AlbaInstance.gc_epoch_tag
            ~gc_epoch:(Int64.succ gc_epoch)
            ~object_id:"" ~version_id:0
            ~chunk_id:0 ~fragment_id:0 in
        Some (wrap_string l, false) in
      let rec inner first =
        alba_client # with_osd
          ~osd_id
          (fun client ->
           (client # namespace_kvs namespace_id) # range
                  Osd.Low
                  ~first ~finc:true ~last
                  ~reverse:false ~max:100)
        >>= fun ((cnt, keys), has_more) ->

        (* TODO could optimize here by grouping together per object_id first *)
        Lwt_list.iter_s
          (fun (gc_tag_key:Osd.key) ->
             let gc_epoch, object_id, chunk_id, fragment_id, version_id =
               AlbaInstance.parse_gc_epoch_tag (Slice.get_string_unsafe gc_tag_key)
             in
             let cleanup_gc_epoch_tag () =
               alba_client # with_osd
                 ~osd_id
                 (fun client ->
                  (client # namespace_kvs namespace_id) # apply_sequence
                         Osd.Low
                         [] [ Osd.Update.delete gc_tag_key; ]
                    >>= fun _success ->
                    (* don't care if it succeeded or not *)
                    Lwt.return ()) in
             let cleanup_fragment () =
               let upds =
                 [ Osd.Update.delete_string
                     (AlbaInstance.fragment
                        ~object_id ~version_id
                        ~chunk_id ~fragment_id);
                   Osd.Update.delete_string
                     (AlbaInstance.fragment_recovery_info
                        ~object_id ~version_id
                        ~chunk_id ~fragment_id);
                   Osd.Update.delete gc_tag_key; ]
               in
               let asserts =
                 [ (* this assert makes sure that if ever a more advanced
                      retag fragments strategy is applied (when an upload
                      failed due to Invalid_gc_epoch) we're not removing
                      a value that could still be used for some object... *)
                   Osd.Assert.value
                     gc_tag_key
                     (Osd.Blob.Bytes ""); ] in

               Lwt_log.debug_f
                 "Cleaning up garbage fragment ns_id=%Li, gc_epoch=%Li, object_id=%S, %i,%i,%i"
                 namespace_id gc_epoch object_id chunk_id fragment_id version_id >>= fun () ->

               alba_client # with_osd
                 ~osd_id
                 (fun client ->
                  (client # namespace_kvs namespace_id) # apply_sequence
                         Osd.Low
                         asserts upds >>= fun _succes ->
                  (* don't care if it succeeded or not *)
                  Lwt.return ()) in
             alba_client # with_nsm_client'
               ~namespace_id
               (fun nsm_client ->
                  nsm_client # get_object_manifest_by_id object_id)
             >>= function
             | None ->
               (* object_id is not yet known on the nsm, or is no longer known
                  either way it's time to clean up this piece of garbage *)
               cleanup_fragment ()
             | Some manifest ->
               (* verify that this (chunk_id, fragment_id) is on the device *)
                let loc = Nsm_model.Manifest.get_location
                            manifest chunk_id fragment_id
                in
                if loc = (Some osd_id, version_id)
                then cleanup_gc_epoch_tag ()
                else cleanup_fragment ())
          keys >>= fun () ->
        if has_more && filter namespace_id
        then inner (List.last keys |> Option.get_some_default first)
        else Lwt.return ()
      in
      let first =
        wrap_string (AlbaInstance.gc_epoch_tag
                       ~gc_epoch:0L
                       ~object_id:"" ~version_id:0
                       ~chunk_id:0 ~fragment_id:0)
      in
      inner first

    method garbage_collect_namespace ?(once=false) ~namespace_id ~grace_period =

      let bump_epoch () =
        let open Nsm_model in
        nsm_host_access # get_namespace_info ~namespace_id
        >>= fun (_, _, gc_epochs) ->

        match GcEpochs.get_latest_valid gc_epochs with
        | None ->
          Lwt.return None
        | Some latest_gc_epoch ->
          alba_client # with_nsm_client'
            ~namespace_id
            (fun nsm -> nsm # enable_gc_epoch (Int64.succ latest_gc_epoch))
          >>= fun () ->

          Lwt_extra2.sleep_approx grace_period >>= fun () ->

          let rec disable_old_epochs epoch =
            if Int64.(epoch >: latest_gc_epoch)
            then Lwt.return ()
            else begin
              alba_client # with_nsm_client'
                ~namespace_id
                (fun nsm -> nsm # disable_gc_epoch epoch) >>= fun () ->
              disable_old_epochs (Int64.succ epoch)
            end in
          disable_old_epochs gc_epochs.GcEpochs.minimum_epoch >>= fun () ->

          (* now we can start collecting everything up to (and including) `latest_gc_epoch` *)

          (* small sanity check *)
          alba_client # with_nsm_client'
            ~namespace_id
            (fun nsm -> nsm # get_gc_epochs) >>= fun gc_epochs ->
          assert (not (GcEpochs.is_valid_epoch gc_epochs latest_gc_epoch));

          Lwt.return (Some latest_gc_epoch)
      in

      if once
      then
        begin
          bump_epoch () >>= function
          | None -> Lwt.return ()
          | Some latest_gc_epoch ->
            nsm_host_access # get_namespace_info ~namespace_id
            >>= fun (_, devices, _) ->
            Lwt_list.iter_p
              (fun osd_id ->
                 self # garbage_collect_device
                   ~osd_id ~namespace_id
                   ~gc_epoch:latest_gc_epoch)
              devices
        end
      else
        begin
          let threads = Hashtbl.create 3 in
          let rec inner () =
            bump_epoch () >>= function
            | None -> Lwt.return ()
            | Some latest_gc_epoch ->
               (if filter namespace_id
                then
                  begin
                    nsm_host_access # get_namespace_info ~namespace_id
                    >>= fun (_, devices, _) ->
                    List.iter
                      (fun osd_id ->
                       if not (Hashtbl.mem threads osd_id)
                       then begin
                           Hashtbl.add threads osd_id ();
                           Lwt.async
                             (fun () ->
                              Lwt_extra2.ignore_errors
                                (fun () ->
                                 self # garbage_collect_device
                                      ~gc_epoch:latest_gc_epoch
                                      ~namespace_id ~osd_id) >>= fun () ->
                              Hashtbl.remove threads osd_id;
                              Lwt.return ())
                         end)
                      devices;
                    Lwt.return_unit
                  end
                else
                  Lwt.return_unit) >>= fun () ->
              Lwt_extra2.sleep_approx (15. *. retry_timeout) >>=
              inner
          in
          inner ()
        end

    method repair_object
             ~namespace_id ~manifest
             ~problem_fragments
      =
      assert (problem_fragments <> []);

      alba_client # get_ns_preset_info ~namespace_id >>= fun preset ->
      alba_client # get_namespace_osds_info_cache ~namespace_id
      >>= fun osd_info_cache ->
      let open Preset in
      let policies = preset.policies in
      let best = Alba_client_common.get_best_policy_exn policies osd_info_cache in
      let (best_k, best_m, _, _), _, _ = best in
      let current_k,current_m =
        let open Nsm_model in
        let es, compression = match manifest.Manifest.storage_scheme with
          | Storage_scheme.EncodeCompressEncrypt (es,c) -> es,c in
        let k,m,w = match es with
          | Encoding_scheme.RSVM (k,m,w) -> (k,m,w) in
        k,m
      in
      let problem_osds =
        List.map
          (fun (chunk_id, fragment_id) ->
            let open Nsm_model in
            let osd_id_o,_ =
              Manifest.get_location manifest chunk_id fragment_id
            in
            osd_id_o
          )
          problem_fragments
        |> List.map_filter_rev Std.id
        |> Int64Set.of_list
      in
      if (current_k,current_m) = (best_k,best_m)
      then
        begin
          nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->
          Lwt.catch
            (fun () ->
             _timed_repair_object_generic_and_update_manifest
               ~namespace_id
               ~manifest
               ~problem_fragments
               ~problem_osds)
            (fun exn ->
             let open Nsm_model.Manifest in
             Lwt_log.info_f
               ~exn
               "Exn while repairing object (~namespace_id:%Li ~object ~name:%S ~object_id:%S), will now try object rewrite"
               namespace_id manifest.name manifest.object_id >>= fun () ->
             Lwt_extra2.ignore_errors
               ~logging:true
               (fun () -> _timed_rewrite_object alba_client ~namespace_id ~manifest))
        end
      else
        _timed_rewrite_object alba_client ~namespace_id ~manifest


    method rebalance_namespace
             ?delay
             ?categorize
             ?only_once
             ~make_first_last_reverse
             ~namespace_id () =
      if maintenance_config.Maintenance_config.enable_rebalance && filter namespace_id
      then self # rebalance_namespace'
                ?delay
                ?categorize
                ?only_once
                ~make_first_last_reverse
                ~namespace_id ()
      else Lwt.return ()

    method rebalance_namespace'
             ?(delay=0.)
             ?(categorize = Rebalancing_helper.categorize)
             ?(only_once = false)
             ~make_first_last_reverse
             ~namespace_id () =
      alba_client # get_namespace_osds_info_cache ~namespace_id
      >>= fun cache ->
      let fill_rates = Rebalancing_helper.calculate_fill_rates cache in
      let too_low, ok, too_high = categorize fill_rates in
      let fr2s = [%show: int * ((int64 * float) list)] in
      Lwt_log.debug_f "too_low:%s ok:%s too_high:%s"
                      (fr2s too_low)
                      (fr2s ok)
                      (fr2s too_high)
      >>= fun () ->
      match too_low,too_high with
      | (0,_),(0,_) -> Lwt_extra2.sleep_approx 60.
      | _ ->
         begin
           let random_osd =
             let n = Hashtbl.length cache in
             let as_list =
               Hashtbl.fold
                 (fun osd_id _ acc -> osd_id :: acc)
                 cache []
             in
             let index = Random.int n in
             List.nth_exn as_list index
           in
           Rebalancing_helper.get_some_manifests
             alba_client
             ~make_first_last_reverse
             ~namespace_id
             random_osd
           >>= fun (n, manifests) ->
           let moves =
             List.fold_left
             (fun acc mf ->
               match
                 Rebalancing_helper.plan_move cache (too_low,ok,too_high) mf
               with
               | None -> acc
               | Some mf -> mf :: acc
             )
             [] manifests
           in
           let moves' = List.sort Rebalancing_helper.compare_moves moves in
           let best = List.take 10 moves' in
           let migrated = ref false in
           let execute_move (manifest,source_osd, target_osd, score )=
             Lwt_extra2.ignore_errors
               ~logging:true
               (fun () ->
                self # rebalance_object
                     ~namespace_id
                     ~manifest
                     ~source_osd ~target_osd
                >>= fun _ ->
                migrated := true;
                Lwt.return ()
                )
           in
           Lwt_log.debug_f "rebalance: moving %i fragments"
                           (List.length best)
           >>= fun ()->
           Lwt_list.iter_s
             (fun move ->
              with_throttling (fun () ->execute_move move)
             )
             best
           >>= fun () ->
           let delay =
             if !migrated
             then
               0. (* go on *)
             else
               let delay' = delay *. 1.5 in
               min 60. (max delay' 1.)
           in
           if only_once
           then Lwt.return ()
           else
             begin
               Lwt_unix.sleep delay >>= fun () ->
               self # rebalance_namespace
                    ~delay
                    ~categorize
                    ~make_first_last_reverse
                    ~namespace_id ()
             end
         end

    method repair_by_policy_namespace ~namespace_id =
      if filter namespace_id
      then self # repair_by_policy_namespace' ~namespace_id ()
      else Lwt.return ()

    method repair_by_policy_namespace' ?(skip_recent=true) ~namespace_id () =

      alba_client # get_ns_preset_info ~namespace_id >>= fun preset ->
      let policies = preset.Preset.policies in
      alba_client # get_namespace_osds_info_cache ~namespace_id >>= fun osds_info_cache ->

      let ((best_k, best_m, _, _) as best_policy),
          best_actual_fragment_count,
          best_actual_max_disks_per_node =
        Alba_client_common.get_best_policy_exn
          policies
          osds_info_cache
      in

      Lwt_log.debug_f
        "Current best policy for namespace %Li = %s, %i"
        namespace_id
        (Policy.show_policy best_policy) best_actual_fragment_count
      >>= fun () ->

      (* 2 phases in repair by policy:
       * 1) repair weakest objects (`Regenerate or `Rewrite), until none are weaker than what's currently possible
       * 2) rebalance based on max_disks_per_node, until all are maximally spread
       *)

      nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->

      nsm # get_stats >>= fun stats ->
      let (_, bucket_count) = stats.Nsm_model.NamespaceStats.bucket_count in

      Lwt_log.debug_f
        "Found buckets %s for namespace_id:%Li"
        ([%show : (Policy.policy * int64) list] bucket_count)
        namespace_id >>= fun () ->

      let buckets =
        bucket_count
        |> List.filter (fun (_, cnt) -> cnt > 0L)
        |> List.map fst
        |> List.map_filter_rev
             (fun ((k, m, fragment_count, max_disks_per_node) as bucket) ->
              if (k, m) = (best_k, best_m)
              then
                begin
                  let open Compare in
                  match Int.compare' fragment_count best_actual_fragment_count with
                  | LT -> Some (bucket, `Regenerate)
                  | EQ
                  | GT ->
                     if max_disks_per_node > best_actual_max_disks_per_node
                     then Some (bucket, `Rebalance)
                     else None
                end
              else
                begin
                  let is_more_preferred_bucket =
                    let rec inner = function
                      | [] -> false
                      | ((k', m', fragment_count', max_disks_per_node') as policy) :: policies ->
                         if policy = best_policy
                         then false
                         else
                           begin
                             if
                               (* does the bucket match this policy? *)
                               k = k'
                               && m = m'
                               && fragment_count >= fragment_count'
                               && max_disks_per_node <= max_disks_per_node'
                             then true
                             else inner policies
                           end
                    in
                    inner policies
                  in
                  if is_more_preferred_bucket
                  then
                    (* this will be handled by another part of the maintenance process
                     * (when osds are considered dead/unavailable for writes for
                     *  a long enough time the objects in these buckets will be
                     *  repaired/rewritten)
                     *)
                    None
                  else
                    Some (bucket, `Rewrite)
                end)
        |> List.sort
             (fun (bucket1, j1) (bucket2, j2) ->
              let compare_bucket_safety (k1, _, fragment_count1, _) (k2, _, fragment_count2, _) =
                (fragment_count1 - k1) - (fragment_count2 - k2)
              in
              let get_max_disks_per_node (_, _, _, x) = x in
              match j1, j2 with
              | `Rebalance, `Rebalance ->
                 compare
                   (get_max_disks_per_node bucket1)
                   (get_max_disks_per_node bucket2)
              | `Regenerate, `Rebalance
              | `Rewrite, `Rebalance ->
                 1
              | `Rebalance, `Regenerate
              | `Rebalance, `Rewrite ->
                 -1
              | `Rewrite,    `Rewrite
              | `Regenerate, `Rewrite
              | `Rewrite,    `Regenerate
              | `Regenerate, `Regenerate ->
                 compare_bucket_safety bucket1 bucket2)
      in

      let repaired_some = ref false in

      let handle_bucket ((k, m, fragment_count, max_disks_per_node), job) =
        nsm # list_objects_by_policy
            ~k ~m
            ~fragment_count
            ~max_disks_per_node
            ~max:200 >>= fun ((cnt, objs), _) ->

        Lwt_log.debug_f
          "repair by policy for namespace_id:%Li, handling bucket (%i,%i,%i,%i), got %i items"
          namespace_id
          k m fragment_count max_disks_per_node
          cnt >>= fun () ->

        let regenerate manifest =
          let _, problem_fragments =
            List.fold_left
              (fun (chunk_id, acc) chunk_location ->
               let actual_fragments = List.map_filter_rev fst chunk_location in
               let actual_fragments_count = List.length actual_fragments in
               let missing_fragments =
                 List.map_filter
                   (fun (fragment_id, (osd_id_o, _)) ->
                    match osd_id_o with
                    | None -> Some (chunk_id, fragment_id)
                    | Some _ -> None)
                   (List.mapi
                      (fun fragment_id osd_id_o -> fragment_id, osd_id_o)
                      chunk_location)
               in
               let to_generate =
                 List.take
                   (best_actual_fragment_count - actual_fragments_count)
                   missing_fragments
               in
               let acc' =
                 List.rev_append
                   to_generate
                   acc
               in
               chunk_id + 1, acc')
              (0, [])
              (Nsm_model.Manifest.locations manifest)
          in
          self # repair_object
               ~namespace_id
               ~manifest
               ~problem_fragments
        in

        let rewrite manifest =
          _timed_rewrite_object alba_client ~namespace_id ~manifest
        in

        let rebalance manifest =
          (* NOTE: rebalance based on node spread (this one)
           *       and rebalance based on disk usage can work
           *       against each other!
           *)
          (* we only look at the node spread of the first chunk.
           * this could be considered a (performance) bug:
           * rebalance would fail and the object would then get rewritten.
           * in practice all chunks will use the same set of osds to store
           * the fragment.
           * if they don't all use the same set of osds then there's a bug
           * in the other rebalance too. (part of the problem is the
           * interface of rebalance object, it takes ~source_osd & ~target_osd
           * as arguments, while it should be about individual fragments
           * that have to be moved elsewhere...)
           *)
          let osds_of_first_chunk =
            List.hd_exn manifest.Nsm_model.Manifest.fragments
            |> List.map Nsm_model.Fragment.loc_of
            |> List.map fst
            |> List.map_filter_rev Std.id
          in

          Lwt_list.map_p
            (fun osd_id ->
             osd_access # get_osd_info ~osd_id >>= fun (osd_info, _, _) ->
             Lwt.return (osd_id, osd_info.Nsm_model.OsdInfo.node_id))
            osds_of_first_chunk
          >>= fun osds_with_node_id ->

          let osds_per_node = Hashtbl.create 3 in
          let () =
            List.iter
              (fun (osd_id, node_id) ->
               let osds = try Hashtbl.find osds_per_node node_id
                          with Not_found -> []
               in
               Hashtbl.replace osds_per_node node_id (osd_id :: osds))
              osds_with_node_id
          in
          let _, source_osd =
            Hashtbl.fold
              (fun node_id osds (cnt, osd_id) ->
               let cnt' = List.length osds in
               if cnt' > cnt
               then cnt', List.hd_exn osds
               else cnt, osd_id)
              osds_per_node
              (0, 0L)
          in
          let osds_to_keep =
            List.filter
              (fun osd_id -> osd_id <> source_osd)
              osds_of_first_chunk
          in
          Maintenance_helper.choose_new_devices
            osd_access
            osds_info_cache
            osds_to_keep
            1
          >>= fun extra_devices ->
          let target_osd = match extra_devices with
            | [ osd_id, _ ] -> osd_id
            | _ -> assert false
          in
          self # rebalance_object
               ~namespace_id
               ~manifest
               ~source_osd
               ~target_osd >>= fun _ ->
          Lwt.return ()
        in

        let t0 = Unix.gettimeofday () in
        let is_recent t = (t0 -. 60.) < t && t < (t0 +. 60.) in

        let do_one mf =
          let wrap_rewrite name f =
            fun manifest ->
            Lwt.catch
              (fun () -> f manifest >>= fun () ->
                         repaired_some := true;
                         Lwt.return_unit)
              (fun exn ->
               Lwt_log.info_f
                 ~exn
                 "%s for object failed, falling back to rewrite" name >>= fun () ->
               Lwt_extra2.ignore_errors
                 ~logging:true
                 (fun () -> rewrite manifest >>= fun () ->
                            repaired_some := true;
                            Lwt.return_unit))
          in
          let f = match job with
            | `Rewrite ->
               rewrite
            | `Regenerate ->
               wrap_rewrite "Regenerate" regenerate
            | `Rebalance ->
               wrap_rewrite "Rebalance" rebalance
          in

          (* filter out recent object uploads to avoid repairing objects
           * that are still being written out lazily
           *)
          if skip_recent && is_recent mf.Nsm_model.Manifest.timestamp
          then Lwt.return_unit
          else f mf
        in

        Lwt_list.iter_s do_one objs >>= fun () ->
        Lwt.return_unit
      in

      let rec loop_buckets = function
        | [] ->
           Lwt_log.debug_f "No (more) buckets to repair by policy for namespace_id:%Li" namespace_id
        | bucket :: buckets ->
           handle_bucket bucket >>= fun () ->
           if !repaired_some
           then Lwt.return_unit
           else loop_buckets buckets
      in
      loop_buckets buckets >>= fun () ->

      if !repaired_some && filter namespace_id
      then self # repair_by_policy_namespace ~namespace_id
      else Lwt.return ()


    method handle_work_item work_item work_id =
      let open Albamgr_protocol.Protocol.Work in
      Lwt_log.debug_f
        "Performing work item %s"
        ([%show : t] work_item) >>= fun () ->

      let wait_until msg check_condition =
        let rec inner () =
          Lwt.catch
            check_condition
            (fun exn ->
               Lwt_log.debug_f ~exn "Exception during %s" msg >>= fun () ->
               Lwt.return false) >>= function
          | false ->
            Lwt_log.debug_f
              "Condition for %s not yet satisfied, trying again in ~%f."
              msg retry_timeout
            >>= fun () ->
            Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
            if filter work_id
            then inner ()
            else Lwt.fail NotMyTask
          | true ->
            Lwt.return ()
        in
        inner ()
      in
      let namespace_exists ~namespace_id =
            Lwt.catch
              (fun () ->
               mgr_access # get_namespace_by_id ~namespace_id
               >>= fun _r ->
               Lwt.return_true
              )
              (let open Albamgr_protocol.Protocol.Error in
               function
                | Albamgr_exn(Namespace_does_not_exist, _) ->
                   Lwt.return_false
                | exn -> Lwt.fail exn
              )
      in
      match work_item with
      | CleanupNsmHostNamespace (nsm_host_id, namespace_id) ->
        let nsm_client = nsm_host_access # get ~nsm_host_id in
        let rec inner () =
          nsm_client # cleanup_for_namespace ~namespace_id >>= fun cnt ->
          Lwt_log.debug_f
            "cleaned %i keys for namespace_id=%Li\n"
            cnt namespace_id
          >>= fun () ->
          if cnt = 0
          then Lwt.return ()
          else if filter work_id
          then inner ()
          else Lwt.fail NotMyTask
        in
        inner ()
      | CleanupOsdNamespace (osd_id, namespace_id) ->
         if Hashtbl.mem purging_osds osd_id
         then Lwt.return ()
         else
           alba_client # with_osd
             ~osd_id
             (fun client ->
              let rec inner first =
                client # delete_namespace namespace_id first >>= function
                | None -> Lwt.return ()
                | Some next ->
                   if not (filter work_id)
                   then Lwt.fail NotMyTask
                   else inner next
              in
              inner (Slice.wrap_string ""))
      | CleanupNamespaceOsd (namespace_id, osd_id) ->
        Lwt.catch
          (fun () ->
             alba_client # with_nsm_client'
               ~namespace_id
               (fun nsm ->
                  let rec inner () =
                    nsm # cleanup_osd_keys_to_be_deleted osd_id >>= fun cnt ->
                    if not (filter work_id)
                    then Lwt.fail NotMyTask
                    else if cnt <> 0
                    then inner ()
                    else Lwt.return ()
                  in
                  inner ()))
          (let open Albamgr_protocol.Protocol.Error in
           let open Nsm_model.Err in
           function
           | Albamgr_exn (Namespace_does_not_exist, _)
           | Nsm_exn (Namespace_id_not_found, _) -> Lwt.return ()
           | exn -> Lwt.fail exn)
      | WaitUntilRepaired (osd_id, namespace_id) ->
        wait_until
          (Printf.sprintf "WaitUntilRepaired osd_id=%Li, namespace_id=%Li" osd_id namespace_id)
          (fun () ->
             Lwt.catch
               (fun () ->
                  alba_client # with_nsm_client'
                    ~namespace_id
                    (fun nsm -> nsm # list_device_objects
                        ~osd_id
                        ~first:"" ~finc:true ~last:None
                        ~max:1
                        ~reverse:false) >>= fun ((cnt, _), _) ->
                  Lwt.return (cnt = 0))
               (let open Albamgr_protocol.Protocol.Error in
                let open Nsm_model.Err in
                function
                | Albamgr_exn (Namespace_does_not_exist, _)
                | Nsm_exn (Namespace_id_not_found, _) -> Lwt.return true
                | exn -> Lwt.fail exn))
      | WaitUntilDecommissioned osd_id ->
        wait_until
          (Printf.sprintf "WaitUntilDecommissioned osd_id=%Li" osd_id)
          (fun () ->
             mgr_access # list_osd_namespaces
               ~osd_id
               ~first:0L ~finc:true ~last:None ~reverse:false ~max:1
             >>= fun ((cnt, _), _) ->
             Lwt.return (cnt = 0))
      | RepairBadFragment (namespace_id, object_id, object_name,
                           chunk_id, fragment_id, version) ->
         begin
          let repair () =
            alba_client # with_nsm_client'
                        ~namespace_id
                        (fun client ->
                         client # get_object_manifest_by_name object_name)
            >>= function
            | None ->
               (* object must've been deleted in the mean time, so no work to do here *)
               Lwt.return ()
            | Some manifest ->
               let open Nsm_model in
               if manifest.Manifest.object_id = object_id
               then
                 begin
                   Lwt_log.warning_f
                     "Repairing object due to bad (missing/corrupted) fragment (%Li, %S, %S, %i, %i)"
                     namespace_id object_id object_name chunk_id fragment_id
                   >>= fun () ->
                   (* this is inefficient but in general it should never happen *)
                   let (_,fragment_version) =
                     Manifest.get_location manifest chunk_id fragment_id
                   in
                   if fragment_version = version
                   then
                   self # repair_object
                        ~namespace_id
                        ~manifest
                        ~problem_fragments:[(chunk_id,fragment_id)]
                   else
                     Lwt_log.warning_f
                       ("not repairing (%Li, %S, %S, %i, %i): "
                        ^^ "version in manifest :%i <> version:%i")
                       namespace_id object_id object_name chunk_id fragment_id
                       fragment_version version
                 end
               else
                 (* object has been replaced with a new version in the mean time,
                  * so no work to do here *)
                 Lwt.return ()
          in
          namespace_exists ~namespace_id >>= function
          | true  -> repair ()
          | false -> Lwt.return ()
         end
      | RewriteObject (namespace_id, object_id) ->
         begin
           let rewrite () =
             alba_client # with_nsm_client'
                         ~namespace_id
                         (fun nsm_client ->
                           nsm_client # get_object_manifest_by_id object_id)
             >>= function
             | None -> Lwt.return ()
             | Some manifest ->
                _timed_rewrite_object alba_client ~namespace_id ~manifest
           in
           namespace_exists ~namespace_id >>= function
           | true -> rewrite ()
           | false -> Lwt.return ()
         end
      | IterNamespaceLeaf (action, namespace_id, name, (first, last)) ->
        let rec inner = function
          | None -> Lwt.return ()
          | Some p ->
             let open Albamgr_protocol.Protocol in
             let fetch ~first ~last =
               alba_client # with_nsm_client'
                           ~namespace_id
                           (fun client ->
                            client # list_objects_by_id
                                   ~first ~finc:true
                                   ~last
                                   ~max:100 ~reverse:false)
             in
             let get_next_batch { Progress.count; next; } =
                (match next, last with
                 | None, _ -> Lwt.return ((0,[]), false)
                 | Some next, None -> fetch ~first:next ~last:None
                 | Some next, Some last ->
                    if next >= last
                    then Lwt.return ((0,[]), false)
                    else fetch ~first:next ~last:(Some (last, false)))
             in
             let get_update_progress_base { Progress.count; _; } ((cnt, objs), has_more) =
                let next =
                  if has_more
                  then Option.map
                         (fun mf -> mf.Nsm_model.Manifest.object_id ^ "\000")
                         (List.last objs)
                  else last
                in
                { Progress.count = Int64.(add count (of_int cnt));
                  next; }
             in
             let update_progress_and_maybe_continue new_p has_more =
               let po = Some new_p in
               mgr_access # update_progress name p po >>= fun () ->

               if not (filter work_id)
               then Lwt.fail NotMyTask
               else if has_more
               then inner po
               else Lwt.return ()
             in

             match p, action with
             | Progress.Rewrite pb, Work.Rewrite ->

                get_next_batch pb >>= fun (((cnt, objs), has_more) as batch) ->

                Lwt_list.iter_s
                  (fun manifest ->
                   _throttled_rewrite_object alba_client
                     ~namespace_id
                     ~manifest)
                  objs >>= fun () ->

                let new_p = Progress.Rewrite (get_update_progress_base pb batch) in

                update_progress_and_maybe_continue
                  new_p
                  has_more
             | Progress.Verify (pb,
                                { Progress.fragments_detected_missing;
                                  fragments_osd_unavailable;
                                  fragments_checksum_mismatch }),
               Work.Verify { checksum; repair_osd_unavailable; } ->
                get_next_batch pb >>= fun (((cnt, objs), has_more) as batch) ->

                Lwt_list.map_s
                  (fun obj ->
                   with_throttling
                     (fun () ->
                      Verify.verify_and_maybe_repair_object
                        alba_client
                        ~namespace_id
                        ~verify_checksum:checksum
                        ~repair_osd_unavailable obj
                      >>= fun (m, u, fcm) ->
                      let per_osd = Int64Map.bindings fcm in
                      Lwt_list.iter_s
                        (fun (osd_id, n_checksum_mismatch) ->
                          alba_client # osd_access # get_osd_info ~osd_id
                          >>= fun (_,state,_) ->
                          let () =
                            Osd_state.add_checksum_errors
                              state
                              (Int64.of_int n_checksum_mismatch)
                          in
                          Lwt.return_unit
                        ) per_osd

                      >>= fun () ->

                      let total =
                        Int64Map.fold
                          (fun osd_id count sum -> sum + count) fcm 0
                      in
                      Lwt.return (m,u,total)
                     )
                  )
                  objs >>= fun res ->

                let fragments_detected_missing,
                    fragments_osd_unavailable,
                    fragments_checksum_mismatch =
                  List.fold_left
                    (fun (a,b,c) (a',b',c') ->

                     let open Int64 in
                     add a (of_int a'),
                     add b (of_int b'),
                     add c (of_int c')
                    )
                    (fragments_detected_missing,
                     fragments_osd_unavailable,
                     fragments_checksum_mismatch)
                    res
                in

                let new_p = Progress.Verify
                              (get_update_progress_base pb batch,
                               { Progress.fragments_detected_missing;
                                 fragments_osd_unavailable;
                                 fragments_checksum_mismatch; }) in

                update_progress_and_maybe_continue
                  new_p
                  has_more
             | _, Work.Rewrite
             | _, Work.Verify _ ->
                Lwt.fail_with "badly set up IterNamespace task!"
        in
        mgr_access # get_progress name >>= fun po ->
        inner po
      | IterNamespace _ ->
        (* the logic for this task is on the albamgr (server) side *)
        Lwt.return ()
      | PropagatePreset preset_name ->
         alba_client # get_preset_cache # refresh ~preset_name


    val mutable next_work_item = 0L
    val work_threads = Hashtbl.create 3

    method add_work_threads work_items =
      let open Albamgr_protocol.Protocol in
      List.iter
        (fun (work_id, work_item) ->
           next_work_item <- Int64.succ work_id;
           let t () =
             let try_do_work () =
               Lwt.catch
                 (fun () ->
                  (if filter work_id
                   then
                     begin
                       Lwt_log.debug_f
                         "Doing work: id=%Li, item=%s"
                         work_id
                         (Work.show work_item) >>= fun () ->

                       self # handle_work_item work_item work_id >>= fun () ->
                       mgr_access # mark_work_completed ~work_id
                     end
                   else
                     Lwt.return_unit)
                  >>= fun () ->
                  Lwt.return `Finished)
                 (function
                   | Lwt.Canceled -> Lwt.fail Lwt.Canceled
                   | exn ->
                     Lwt_log.info_f
                       "Got exception %s while working on item %Li"
                       (Printexc.to_string exn)
                       work_id >>= fun () ->
                     Lwt.return `Retry)
             in
             let rec inner backoff =
               try_do_work ()
               >>= function
               | `Finished ->
                 Lwt.return ()
               | `Retry ->
                 Lwt_unix.sleep backoff >>= fun () ->
                 inner (min (backoff *. 1.5) 120.)
             in
             Lwt.finalize
               (fun () -> inner 1.0)
               (fun () ->
                  Lwt_log.debug_f
                    "Finished work item %Li"
                    work_id >>= fun () ->
                  if not (Hashtbl.mem work_threads work_id)
                  then
                    Lwt_log.warning_f
                      "No entry in hashtbl for this workthread %Li"
                      work_id
                  else begin
                    Hashtbl.remove work_threads work_id;
                    Lwt.return ()
                  end)
           in
           if not (Hashtbl.mem work_threads work_id)
           then begin
             Hashtbl.add work_threads work_id ();
             Lwt.ignore_result (t ())
           end)
        work_items


    method report_stats (delay:float) : unit Lwt.t =
      let msg = "problem with stats?"
      and section = MStats.section
      in
      let f () =
        MStats.stop ();
        Lwt_log.info_f
          ~section
          "statistics:%s"
          (MStats.show_stats ())
      in
      Lwt_extra2.run_forever msg f delay


  method do_work ?(once = false) () : unit Lwt.t =

    coordinator # add_on_position_changed (fun () -> next_work_item <- 0L);

    let rec inner () =
      mgr_access # get_work
                  ~first:next_work_item ~finc:true
                  ~last:None ~max:100 ~reverse:false
      >>= fun ((cnt, work_items), has_more) ->
      Lwt_log.debug_f "Adding work, got %i items" cnt >>= fun () ->
      self # add_work_threads work_items;

      if has_more || (once && cnt > 0)
      then inner ()
      else Lwt.return_unit
    in

    if once
    then begin
      inner () >>= fun () ->
      let rec wait_for_work_threads () =
        if Hashtbl.length work_threads = 0
        then Lwt.return ()
        else begin
          Lwt_log.debug_f
            "Still waiting for %i threads (%s)"
            (Hashtbl.length work_threads)
            ([%show : int64 list]
               (Hashtbl.fold
                  (fun id _ acc -> id :: acc)
                  work_threads
                  []))
          >>= fun () ->
          Lwt_unix.sleep 0.2 >>= fun () ->
          wait_for_work_threads ()
        end in
      Lwt.pick [
        wait_for_work_threads ();
        begin
          let rec i () =
            Lwt_unix.sleep retry_timeout >>= fun () ->
            inner () >>= fun () ->
            i ()
          in
          i ()
        end;
      ]
    end else
      Lwt_extra2.run_forever
        (Printf.sprintf "Got unexpected exception in main do_work thread")
        inner
        retry_timeout

  method maintenance_for_namespace
           ~namespace
           ~namespace_id
           ~namespace_info
    =
    Lwt_extra2.ignore_errors
      (fun () ->
         let open Albamgr_protocol.Protocol in

         let rec wait_until_ns_active current =
           begin
             match current with
             | None -> Lwt.return `Stop
             | Some (_ ,ns_info) ->
                let open Namespace in
                Lwt.return
                  (match ns_info.state with
                   | Active     -> `Continue ns_info
                   | Creating   -> `Retry
                   | Recovering -> `Retry
                   | Removing   -> `Stop
                  )
           end
           >>= function
           | `Stop     -> Lwt.fail (Failure "Namespace no longer exists")
           | `Continue ns_info -> Lwt.return ns_info
           | `Retry ->
              Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
              Lwt.catch
                (fun () -> mgr_access # get_namespace ~namespace)
                (fun exn ->  Lwt.return current)
              >>= wait_until_ns_active
         in
         let current = Some (namespace, namespace_info) in
         wait_until_ns_active current >>= fun ns_info ->

         nsm_host_access # maybe_update_namespace_info
           ~namespace_id
           ns_info >>= fun _ ->

         let rec run_until_removed (msg, f) =
           Lwt.catch
             (fun () ->
              f () >>= fun () ->
              Lwt.return `Continue)
             (function
               | Error.Albamgr_exn (Error.Namespace_does_not_exist, _)
               | Nsm_model.Err.Nsm_exn (Nsm_model.Err.Namespace_id_not_found, _) ->
                 Lwt.return `Stop
               | exn ->
                 Lwt_log.debug_f
                   ~exn
                   "Exception during %s maintenance for %S %Li"
                   msg namespace namespace_id >>= fun () ->
                 Lwt.return `Continue)
           >>= function
           | `Stop -> Lwt.return ()
           | `Continue ->
             Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
             run_until_removed (msg, f)
         in
         let tasks =
           [
               "clean obsolete fragments",
               (fun () ->
                self # clean_obsolete_keys_namespace ~once:false ~namespace_id);
               "garbage collect",
               (fun () ->
                self # garbage_collect_namespace
                     ~once:false
                     ~grace_period:gc_grace_period
                     ~namespace_id);
               "repair by policy",
               (fun () -> self # repair_by_policy_namespace ~namespace_id);
               "rebalance",
               (fun () -> self # rebalance_namespace
                               ~make_first_last_reverse
                               ~namespace_id ());
             ]
         in
         Lwt.join (List.map run_until_removed tasks)
      )


  method maintenance_for_all_namespaces : unit Lwt.t =
    let next_id = ref 0L in

    let rec inner () =
      Lwt.catch
        (fun () ->
           mgr_access # list_namespaces_by_id
             ~first:!next_id ~finc:true ~last:None
             ~max:100
           >>= fun ((cnt, namespaces), has_more) ->

           List.iter
             (fun (namespace_id, namespace, namespace_info) ->
                next_id := Int64.succ namespace_id;
                Lwt.async
                  (fun () ->
                   self # maintenance_for_namespace
                        ~namespace ~namespace_id ~namespace_info)
             )
             namespaces;

           Lwt.return has_more)
        (fun exn ->
           Lwt.return false) >>= fun has_more ->
      (if has_more
       then Lwt.return ()
       else Lwt_extra2.sleep_approx retry_timeout) >>= fun () ->
      inner ()
    in

    inner ()

  method deliver_all_messages ?(is_master= fun () -> true) () : unit Lwt.t =
    Alba_client_message_delivery.deliver_all_messages
      is_master
      mgr_access
      nsm_host_access
      osd_access

  method cache_eviction () : unit Lwt.t =
    let get_prefixes () =
      Hashtbl.fold
        (fun prefix _ acc -> prefix :: acc)
        maintenance_config.Maintenance_config.cache_eviction_prefix_preset_pairs
        []
    in
    let do_random_eviction () =
      Alba_eviction.do_random_eviction
        alba_client
        ~prefixes:(get_prefixes ())
    in

    let rec get_redis_lru_cache_eviction () =
      Lwt.catch
        (fun () ->
         mgr_access # get_maintenance_config >>= fun r ->
         Lwt.return (`Success r.Maintenance_config.redis_lru_cache_eviction))
        (fun exn ->
         Lwt.return `Retry) >>= function
      | `Success x -> Lwt.return x
      | `Retry ->
         Lwt_unix.sleep 10. >>= fun () ->
         get_redis_lru_cache_eviction ()
    in
    get_redis_lru_cache_eviction () >>= function
    | None ->
       Lwt_extra2.run_forever
         "random-eviction"
         (fun () ->
          if Alba_eviction.should_evict alba_client coordinator
          then do_random_eviction ()
          else Lwt.return ())
         retry_timeout
    | Some { Maintenance_config.host; port; key; } ->
       let t1 =
         Lwt_extra2.run_forever
           "redis-lru-eviction"
           (fun () ->
            if Alba_eviction.should_evict alba_client coordinator
            then
              begin
                Lwt.catch
                  (fun () ->
                   let module R = Redis_lwt.Client in
                   R.with_connection
                     R.({ host; port; })
                     (fun client ->
                      let total_disk_size =
                        Hashtbl.fold
                          (fun _ (osd_info, _, _) acc ->
                           Int64.add acc osd_info.Nsm_model.OsdInfo.total)
                          (osd_access # osds_info_cache)
                          0L
                      in
                      (* collect 1% of objects *)
                      let target = Int64.div total_disk_size 100L in
                      let rec inner acc =
                        if acc > target
                        then Lwt.return ()
                        else
                          begin
                            Alba_eviction.lru_collect_some_garbage
                              alba_client
                              client
                              key >>= fun size ->
                            inner (Int64.add acc size)
                          end
                      in
                      inner 0L)
                  )
                  (fun exn ->
                   Lwt_log.info_f
                     ~exn
                     "redis based lru eviction failed, doing fallback to random" >>= fun () ->
                   do_random_eviction ())
              end
            else
              Lwt.return ())
           retry_timeout
       in

       let t2 =
         Lwt_extra2.run_forever
           "redis-lru-eviction-garbage-collect"
           (fun () ->
            let rec inner delay =
              let delay = max delay 1. in
              Lwt.catch
                (fun () ->
                 let prefixes = get_prefixes () in
                 Lwt_list.map_p
                   (fun prefix ->
                    mgr_access # list_all_namespaces_with_prefix prefix)
                   prefixes >>= fun namespaces ->
                 let namespaces =
                   List.fold_left
                     (fun acc (_, namespaces) ->
                      List.rev_append namespaces acc)
                     []
                     namespaces
                 in
                 let length = List.length namespaces in
                 if length > 0
                 then
                   begin
                     let (namespace, ns_info) = List.nth_exn namespaces (Random.int length) in
                     let namespace_id = ns_info.Albamgr_protocol.Protocol.Namespace.id in
                     nsm_host_access # get_nsm_by_id ~namespace_id
                     >>= fun nsm_client ->
                     let first, last, reverse = make_first_last_reverse () in
                     nsm_client # list_objects_by_id
                                ~first ~finc:true ~last
                                ~reverse ~max:200 >>= fun ((_, mfs), _) ->
                     if mfs = []
                     then
                       begin
                         (* maybe the namespace was empty ...
                          * if so detect it and throw it away
                          *)
                         nsm_client # list_objects_by_id
                                    ~first:"" ~finc:true ~last:None
                                    ~reverse:false ~max:1 >>= fun ((_, mfs), _) ->
                         if mfs = []
                         then
                           begin
                             alba_client # delete_namespace ~namespace >>= fun () ->
                             Lwt.return 0.
                           end
                         else
                           Lwt.return (delay *. 1.5)
                       end
                     else
                       begin
                         let items = List.map
                                       (fun mf ->
                                         let open Nsm_model.Manifest in
                                         mf.timestamp,
                                         serialize
                                           (Llio.tuple3_to
                                              Llio.int8_to
                                              x_int64_to
                                              Llio.string_to)
                                           (1, namespace_id, mf.name))
                                       mfs
                         in
                         let module R = Redis_lwt.Client in
                         R.with_connection
                           R.({ host; port; })
                           (fun client ->
                             Redis_lwt.Client.zadd
                               client
                               key
                               ~x:`NX
                               items)
                         >>= fun count ->
                         if count = 0
                         then
                           Lwt.return (delay *. 1.5)
                         else
                           (* found garbage, scan again for more... *)
                           Lwt.return 0.
                       end
                   end
                 else
                   Lwt.return (delay *. 1.5)
                )
                (fun exn ->
                 Lwt_log.info_f ~exn "Exception during redis-lru-eviction-garbage-collect" >>= fun () ->
                 Lwt.return (delay *. 1.5)) >>= fun delay ->
              let delay = min 60. delay in
              Lwt_extra2.sleep_approx delay >>= fun () ->
              inner delay
            in
            inner 1.)
           retry_timeout
       in

       Lwt.choose [ t1; t2; ]

  method refresh_alba_osd_cfgs : unit Lwt.t =
    let t () =
      Hashtbl.map_filter
        (fun osd_id (osd_info, _, _) ->
         let open Nsm_model.OsdInfo in
         match osd_info.kind with
         | Asd _
         | Kinetic _ ->
            None
         | Alba cfg
         | Alba2 cfg ->
            Some (osd_id, osd_info, cfg)
         | AlbaProxy _ ->
            None
        )
        (osd_access # osds_info_cache)
      |> Lwt_list.filter_map_p
           (fun (osd_id, osd_info, alba_cfg) ->
            let alba_osd_arakoon_cfg = alba_cfg.Nsm_model.OsdInfo.cfg in
            Lwt.catch
              (fun () ->
               Lwt.catch
                 (fun () ->
                  Albamgr_client.with_client'
                    alba_osd_arakoon_cfg
                    ~tls_config
                    (fun client ->
                     client # get_client_config))
                 (fun exn ->
                  Albamgr_client.retrieve_cfg_from_any_node
                    ~tls_config
                    alba_osd_arakoon_cfg >>= function
                  | Albamgr_client.Retry ->
                     Lwt.fail_with
                       "Got Retry from Albamgr_client.retrieve_cfg_from_any_node"
                  | Albamgr_client.Res x -> Lwt.return x)
               >>= fun alba_osd_arakoon_cfg' ->
               if alba_osd_arakoon_cfg' = alba_osd_arakoon_cfg
               then Lwt.return_none
               else Lwt.return (Some (Nsm_model.OsdInfo.(get_long_id osd_info.kind),
                                      let open Albamgr_protocol.Protocol.Osd.Update in
                                      make ~albamgr_cfg':alba_osd_arakoon_cfg' ()))
              )
              (fun exn ->
               Lwt_log.info_f
                 ~exn
                 "Failed to refresh abm_cfg for osd %Li" osd_id >>= fun () ->
               Lwt.return_none))
      >>= fun updates ->
      mgr_access # update_osds updates
    in
    Lwt_extra2.run_forever
      "refresh_alba_osd_cfg"
      (fun () ->
       if coordinator # is_master
       then t ()
       else Lwt.return_unit)
      retry_timeout
end
