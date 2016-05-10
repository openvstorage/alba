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
  let coordinator =
    let open Maintenance_coordination in
    make_maintenance_coordinator
      (alba_client # mgr_access)
      ~lease_name:maintenance_lease_name
      ~lease_timeout:maintenance_lease_timeout
      ~registration_prefix:maintenance_registration_prefix
  in
  let () = coordinator # init in
  let filter item_id =
    (* item_id could be e.g. namespace_id or work item id *)
    let remainder = (Int32.to_int item_id) mod coordinator # get_modulo
    in remainder = coordinator # get_remainder
  in

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
    MStats.new_delta MStats.REPAIR_GENERIC_COUNT (Int32Set.cardinal problem_osds |> float);
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
                            enable_rebalance = false; })

    method get_maintenance_config = maintenance_config

    method refresh_maintenance_config : unit Lwt.t =
      Lwt_extra2.run_forever
        "refresh_maintenance_config"
        (fun () ->
         alba_client # mgr_access # get_maintenance_config >>= fun cfg ->
         maintenance_config <- cfg;
         Lwt.return_unit)
        retry_timeout

    val purging_osds = Hashtbl.create 3
    method refresh_purging_osds ?(once = false) () : unit Lwt.t =
      let inner () =
        alba_client # mgr_access # list_all_purging_osds >>= fun (_, purging_osds') ->
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
      alba_client # osd_access # get_osd_info ~osd_id >>= fun (osd_info, _) ->
      Lwt.return ((maintenance_config.Maintenance_config.enable_auto_repair
                   && Hashtbl.mem maybe_dead_osds osd_id)
                  || osd_info.Nsm_model.OsdInfo.decommissioned)

    method failure_detect_all_osds : unit Lwt.t =
      Lwt_extra2.run_forever
        "failure_detect_all_osds"
        (fun () ->
         if maintenance_config.Maintenance_config.enable_auto_repair
         then
           begin
             alba_client # mgr_access # list_all_claimed_osds >>= fun (_, osds) ->

             (* failure detecting already decommissioned osds isn't that useful *)
             let osds =
               List.filter
                 (fun (_, osd_info) -> not osd_info.Nsm_model.OsdInfo.decommissioned)
                 osds
             in

             Lwt.async
               (fun () ->
                Automatic_repair.periodic_load_osds
                  alba_client
                  maintenance_config
                  osds);

             let past_date =
               Unix.gettimeofday () -.
                 maintenance_config.Maintenance_config.auto_repair_timeout_seconds
             in
             List.iter
               (fun (osd_id, osd_info) ->
                let open Nsm_model.OsdInfo in
                (* TODO allow disabling auto repair for other failure domains? *)
                if not (Automatic_repair.recent_enough past_date osd_info.write
                        && Automatic_repair.recent_enough past_date osd_info.write)
                   && not (List.mem
                             osd_info.node_id
                             maintenance_config.Maintenance_config.auto_repair_disabled_nodes)
                then Hashtbl.replace maybe_dead_osds osd_id ()
                else Hashtbl.remove maybe_dead_osds osd_id)
               osds;

             Lwt.return ()
           end
         else
           Lwt.return ())
        retry_timeout

    method rebalance_object
             ~(namespace_id:int32)
             ~manifest
             ~(source_osd:Nsm_model.osd_id)
             ~(target_osd:Nsm_model.osd_id)
           : (int * int * Nsm_model.osd_id * Nsm_model.osd_id) list Lwt.t
      =
      let open Nsm_model in
      let open Manifest in
      let locations = manifest.fragment_locations in
      let osds_touched = Manifest.osds_used manifest.fragment_locations in

      let () =
        if DeviceSet.mem target_osd osds_touched
        then
          let msg =
            Printf.sprintf
              "bad move: source:%li => target:%li touched:%s"
              source_osd target_osd
              ([%show : int32 list] (DeviceSet.elements osds_touched))
          in
          failwith msg
      in
      let fragment_checksums = manifest.fragment_checksums in
      let object_id = manifest.object_id in
      let object_name = manifest.name in

      Lwt_log.debug_f
        "rebalance_object %S ~source_osd:%li ~target_osd:%li"
        object_name source_osd target_osd
      >>= fun () ->

      let version_id0 = manifest.version_id in
      let version_id1 = version_id0 + 1 in
      let fragment_info =
        Layout.combine
          locations
          fragment_checksums
      in
      alba_client # get_ns_preset_info ~namespace_id
      >>= fun preset ->

      alba_client # nsm_host_access # get_gc_epoch ~namespace_id
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
                let ((osd_id_o, version), checksum) = f in
                if osd_id_o = Some source_osd
                then (i, checksum, version)
                else _find (i+1) fs
           in _find 0 chunk_location
         in
         let (fragment_id, fragment_checksum,version) = source_fragment in

         let object_info_o =
           let is_last_chunk = chunk_id = List.length locations - 1 in
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
           object_name
           object_id
           object_info_o
           encryption
           (List.nth_exn manifest.chunk_sizes chunk_id)
           (List.nth_exn manifest.fragment_packed_sizes chunk_id)
           (List.nth_exn manifest.fragment_checksums chunk_id)
         >>= fun recovery_info_slice ->

         let open Alba_client_errors.Error in

         (* TODO get unpacked fragment from cache if available? *)
         Alba_client_download.download_packed_fragment
           (alba_client # osd_access)
           ~location:(List.nth_exn chunk_location fragment_id |> fst)
           ~namespace_id
           ~object_id ~object_name
           ~chunk_id ~fragment_id >>= function
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
                     (alba_client # osd_access)
                     ~namespace_id ~object_id
                     ~chunk_id ~fragment_id ~version_id:version_id1
                     ~packed_fragment
                     ~checksum:fragment_checksum
                     ~gc_epoch
                     ~recovery_info_blob:(Osd.Blob.Slice recovery_info_slice)
                     ~osd_id:target_osd
                   >>= fun () ->
                   Lwt.return (chunk_id, fragment_id, source_osd, target_osd)
                 end)
              (fun () ->
               Lwt_bytes.unsafe_destroy packed_fragment;
               Lwt.return_unit)
         )
        (List.mapi (fun i lc -> i, lc) fragment_info)
      >>= fun object_location_movements ->
      Lwt_log.debug_f "object_location_movements: %s"
        ([%show: (int * int * osd_id * osd_id ) list] object_location_movements)
      >>= fun ()->
      let updated_object_locations =
        List.map
          (fun (c,f,s, target_osd_id) ->
             (c,f, Some target_osd_id))
          object_location_movements
      in
      alba_client # with_nsm_client'
        ~namespace_id
        (fun client ->
         client # update_manifest
                ~object_name
                ~object_id
                updated_object_locations
                ~gc_epoch ~version_id:version_id1
        )
      >>= fun () ->
        Lwt_list.iter_p
        (fun (chunk_id, fragment_id, source_osd, target_osd) ->
         alba_client # with_osd
           ~osd_id:source_osd
           (fun osd_client ->
            let key =
              Osd_keys.AlbaInstance.fragment
                ~namespace_id ~object_id ~version_id:version_id0
                ~chunk_id ~fragment_id
            in
            osd_client # apply_sequence
                       Osd.Low
                       [] [Osd.Update.delete_string key]
            >>= fun s ->
            match s with
            | Osd.Ok    -> Lwt.return ()
            | Osd.Exn e ->
               Lwt_log.info_f
                 ("object: %S" ^^
                 "delete fragment(chunk_id:%i,fragment_id:%i,osd_id:%li): %s" ^^
                 " => garbage")
                 object_id chunk_id fragment_id source_osd
                 ([%show: Osd.Error.t ] e)
           )
        ) object_location_movements
      >>= fun () ->
        let size = List.length object_location_movements in
        let () = MStats.new_delta MStats.REBALANCE (float size) in
        Lwt.return object_location_movements


    method decommission_device
        ?(deterministic=false)
        ~namespace_id
        ~osd_id ()
      =
      Lwt_log.debug_f
        "Decommissioning osd %li namespace_id:%li"
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
      Lwt_log.debug_f
        "Decommissioning osd:%li namespace_id:%li first:%S ~reverse:%b cnt:%i, has_more:%b"
        osd_id namespace_id first reverse cnt has_more
      >>= fun () ->
      Lwt_list.iter_s
        (fun manifest ->
         if Hashtbl.mem purging_osds osd_id
         then
           alba_client # nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->
           alba_client # with_nsm_client'
             ~namespace_id
             (fun client ->
              let open Nsm_model.Manifest in
              let _, updated_locations =
                List.fold_left
                  (fun (chunk_id, acc) fragment_locations ->
                   List.fold_left
                     (fun (fragment_id, acc) (osd_id_o, version) ->
                      let acc = match osd_id_o with
                        | None -> acc
                        | Some osd_id ->
                           if Hashtbl.mem purging_osds osd_id
                           then (chunk_id, fragment_id, None) :: acc
                           else acc
                      in
                      (fragment_id + 1, acc))
                     (0, acc)
                     fragment_locations)
                  (0, [])
                  manifest.fragment_locations
              in
              client # update_manifest
                ~object_name:manifest.name
                ~object_id:manifest.object_id
                updated_locations
                ~gc_epoch
                ~version_id:(manifest.version_id + 1))
         else
           Lwt.catch
             (fun () ->
              _timed_repair_object_generic_and_update_manifest
                ~namespace_id
                ~manifest
                ~problem_fragments:[]
                ~problem_osds:(Int32Set.of_list [ osd_id ])
             )
             (fun exn ->
              let open Nsm_model.Manifest in
              Lwt_log.info_f
                ~exn
                "Exn while repairing osd %li (~namespace_id:%li ~object ~name:%S ~object_id:%S), will now try object rewrite"
                osd_id namespace_id manifest.name manifest.object_id >>= fun () ->
              Lwt_extra2.ignore_errors
                ~logging:true
                (fun () -> _timed_rewrite_object alba_client ~namespace_id ~manifest)
             )
        )
        manifests >>= fun () ->

      self # should_repair ~osd_id >>= fun should_repair ->
      if has_more && should_repair
      then self # decommission_device ~deterministic ~namespace_id ~osd_id ()
      else Lwt.return ()

    method repair_osd ~osd_id =
      alba_client # mgr_access # list_all_osd_namespaces ~osd_id >>= fun (_, namespaces) ->

      Lwt_list.iter_p
        (fun namespace_id ->
           Lwt_extra2.ignore_errors
             (fun () ->
                self # decommission_device
                  ~namespace_id
                  ~osd_id ()))
        namespaces

    method repair_osds : unit Lwt.t =
      let threads = Hashtbl.create 3 in
      let rec inner () =
        alba_client # mgr_access # list_all_decommissioning_osds >>= fun (_, osds) ->

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
        let upds = List.map Osd.Update.delete_string
            keys_to_be_deleted
        in

        Lwt_log.debug_f
          "Cleaning obsolete keys for osd_id=%li %S"
          osd_id
          ([%show : string list] keys_to_be_deleted) >>= fun () ->
        Prelude.with_timing_lwt
          (fun () ->
           alba_client # with_osd
                       ~osd_id
                       (fun osd_client ->
                        osd_client # apply_sequence
                                   Osd.Low
                                   [] upds >>= function
                        | Osd.Ok -> Lwt.return ()
                        | Osd.Exn x ->
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
          alba_client # nsm_host_access # get_namespace_info ~namespace_id >>= fun (_, devices, _) ->

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
                 alba_client # nsm_host_access # get_namespace_info ~namespace_id >>= fun (_, osds, _) ->
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
            ~namespace_id ~gc_epoch:(Int64.succ gc_epoch)
            ~object_id:"" ~version_id:0
            ~chunk_id:0 ~fragment_id:0 in
        Some (wrap_string l, false) in
      let rec inner first =
        alba_client # with_osd
          ~osd_id
          (fun client ->
           client # range
                  Osd.Low
                  ~first ~finc:true ~last
                  ~reverse:false ~max:100)
        >>= fun ((cnt, keys), has_more) ->

        (* TODO could optimize here by grouping together per object_id first *)
        Lwt_list.iter_s
          (fun (gc_tag_key:Osd.key) ->
             let ns', gc_epoch, object_id, chunk_id, fragment_id, version_id =
               AlbaInstance.parse_gc_epoch_tag (Slice.get_string_unsafe gc_tag_key)
             in
             let cleanup_gc_epoch_tag () =
               alba_client # with_osd
                 ~osd_id
                 (fun client ->
                  client # apply_sequence
                         Osd.Low
                         [] [ Osd.Update.delete gc_tag_key; ]
                    >>= fun _success ->
                    (* don't care if it succeeded or not *)
                    Lwt.return ()) in
             let cleanup_fragment () =
               let upds =
                 [ Osd.Update.delete_string
                     (AlbaInstance.fragment
                        ~namespace_id
                        ~object_id ~version_id
                        ~chunk_id ~fragment_id);
                   Osd.Update.delete_string
                     (AlbaInstance.fragment_recovery_info
                        ~namespace_id
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
                 "Cleaning up garbage fragment ns_id=%li, gc_epoch=%Li, object_id=%S, %i,%i,%i"
                 namespace_id gc_epoch object_id chunk_id fragment_id version_id >>= fun () ->

               alba_client # with_osd
                 ~osd_id
                 (fun client ->
                    client # apply_sequence Osd.Low asserts upds >>= fun _succes ->
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
               let locs = manifest.Nsm_model.Manifest.fragment_locations in
               let loc = List.nth_exn (List.nth_exn locs chunk_id) fragment_id in
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
                       ~namespace_id ~gc_epoch:0L
                       ~object_id:"" ~version_id:0
                       ~chunk_id:0 ~fragment_id:0)
      in
      inner first

    method garbage_collect_namespace ?(once=false) ~namespace_id ~grace_period =

      let bump_epoch () =
        let open Nsm_model in
        alba_client # nsm_host_access # get_namespace_info ~namespace_id
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
            alba_client # nsm_host_access # get_namespace_info ~namespace_id
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
                    alba_client # nsm_host_access # get_namespace_info ~namespace_id
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
      let open Albamgr_protocol.Protocol.Preset in
      let policies = preset.policies in
      let best = Alba_client_common.get_best_policy_exn policies osd_info_cache in
      let (best_k, best_m, _, _), _ = best in
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
          (fun (chunk_id,fragment_id) ->
           let open Nsm_model in
           let chunk_fgs =
             List.nth_exn manifest.Manifest.fragment_locations chunk_id in
           let osd_id,_ = List.nth_exn chunk_fgs fragment_id in
           osd_id
          )
          problem_fragments
        |> List.map_filter_rev Std.id
        |> Int32Set.of_list
      in
      if (current_k,current_m) = (best_k,best_m)
      then
        begin
          alba_client # nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->
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
               "Exn while repairing object (~namespace_id:%li ~object ~name:%S ~object_id:%S), will now try object rewrite"
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
      let fr2s = [%show: int * ((int32 * float) list)] in
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
      then self # repair_by_policy_namespace' ~namespace_id
      else Lwt.return ()

    method repair_by_policy_namespace' ~namespace_id =
      (* TODO this should instead repair by the number of
       * spare fragments instead (weakest objects first),
       * and only after that do a repair by policy
       *)

      alba_client # get_ns_preset_info ~namespace_id >>= fun preset ->
      let policies =
        let open Albamgr_protocol.Protocol.Preset in
        preset.policies
      in
      alba_client # get_namespace_osds_info_cache ~namespace_id >>= fun osds_info_cache ->

      let best_policy, best_actual_fragment_count =
        Alba_client_common.get_best_policy_exn
          policies
          osds_info_cache
      in

      Lwt_log.debug_f
        "Current best policy for namespace %li = %s, %i"
        namespace_id
        (Policy.show_policy best_policy) best_actual_fragment_count
      >>= fun () ->

      (* first get all buckets
       * - for non existing buckets -> rewrite all objects
       * - for existing buckets: iter from worst until best policy
       *   rewrite objects in weak buckets
       *   for current best policy: do orange repair
       *)

      alba_client # nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->

      nsm # get_stats >>= fun stats ->
      let (_, bucket_counts) = stats.Nsm_model.NamespaceStats.bucket_count in

      let policies_rev = List.rev policies in

      let policies_rev =
        List.fold_left
          (fun acc ((k,m,_,_), cnt) ->
           if cnt > 0L
           then
             begin
               if List.exists
                    (fun (k', m', _, _) -> k' = k && m' = m)
                    acc
               then acc
               else (k,m,0,0) :: acc
             end
           else
             acc)
          policies_rev
          bucket_counts
      in

      let rec inner = function
        | [] -> Lwt.fail_with "should never get here"
        | policy :: tl ->
          let k, m, _, _ = policy in
          if policy = best_policy
          then begin
            nsm # list_objects_by_policy'
              ~first:((k,m,0,0), "") ~finc:true
              ~last:(Some (((k, m, best_actual_fragment_count, 0), ""), false))
              ~max:200 >>= fun ((cnt, objs), _) ->

            let do_one manifest =
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
                   manifest.Nsm_model.Manifest.fragment_locations
               in
               self # repair_object
                    ~namespace_id
                    ~manifest
                    ~problem_fragments
            in
            Lwt_list.iter_s do_one objs >>= fun () ->
            Lwt.return (cnt > 0)
          end
          else begin
            nsm # list_objects_by_policy ~k ~m ~max:200 >>= fun ((cnt, objs), _) ->
            if cnt > 0
            then
              Lwt_list.iter_s
                (fun manifest -> _throttled_rewrite_object alba_client ~namespace_id ~manifest)
                objs >>= fun () ->
              Lwt.return true
            else
              inner tl
          end
      in

      inner policies_rev >>= fun repaired_some ->

      if repaired_some && filter namespace_id
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
               alba_client # mgr_access # get_namespace_by_id ~namespace_id
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
        let nsm_client = alba_client # nsm_host_access # get ~nsm_host_id in
        let rec inner () =
          nsm_client # cleanup_for_namespace ~namespace_id >>= fun cnt ->
          Lwt_log.debug_f
            "cleaned %i keys for namespace_id=%li\n"
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
              let namespace_prefix =
                serialize
                  Osd_keys.AlbaInstance.namespace_prefix_serializer
                  namespace_id
              in
              let namespace_prefix' = Slice.wrap_string namespace_prefix in
              let namespace_next_prefix =
                match (Key_value_store.next_prefix namespace_prefix) with
                | None -> None
                | Some (p,b) -> Some (Slice.wrap_string p, b)
              in
              let rec inner (first, finc) =
                client # range
                       Osd.Low
                       ~first ~finc
                       ~last:namespace_next_prefix
                       ~max:200 ~reverse:false >>= fun ((cnt, keys), has_more) ->
                client # apply_sequence
                       Osd.Low
                       []
                       (List.map
                          (fun k -> Osd.Update.delete k)
                          keys) >>= fun _ ->
                if not (filter work_id)
                then Lwt.fail NotMyTask
                else if has_more
                then inner (List.last keys |> Option.get_some_default namespace_prefix', false)
                else Lwt.return ()
              in
              inner (namespace_prefix', true))
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
          (Printf.sprintf "WaitUntilRepaired osd_id=%li, namespace_id=%li" osd_id namespace_id)
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
          (Printf.sprintf "WaitUntilDecommissioned osd_id=%li" osd_id)
          (fun () ->
             alba_client # mgr_access # list_osd_namespaces
               ~osd_id
               ~first:0l ~finc:true ~last:None ~reverse:false ~max:1
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
               if manifest.Nsm_model.Manifest.object_id = object_id
               then begin
                   Lwt_log.warning_f
                     "Repairing object due to bad (missing/corrupted) fragment (%li, %S, %S, %i, %i)"
                     namespace_id object_id object_name chunk_id fragment_id
                   >>= fun () ->
                   (* this is inefficient but in general it should never happen *)
                   self # repair_object
                        ~namespace_id
                        ~manifest
                        ~problem_fragments:[(chunk_id,fragment_id)]
                 end else
                 (* object has been replaced with a new version in the mean time,
               so no work to do here *)
                 Lwt.return ()
          in
          namespace_exists ~namespace_id >>= function
          | true  -> repair ()
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
               alba_client # mgr_access # update_progress
                           name p po >>= fun () ->

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
                     add c (of_int c'))
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
        alba_client # mgr_access # get_progress name >>= fun po ->
        inner po
      | IterNamespace _ ->
        (* the logic for this task is on the albamgr (server) side *)
        Lwt.return ()


    val mutable next_work_item = 0l
    val work_threads = Hashtbl.create 3

    method add_work_threads work_items =
      let open Albamgr_protocol.Protocol in
      List.iter
        (fun (work_id, work_item) ->
           next_work_item <- Int32.succ work_id;
           let t () =
             let try_do_work () =
               Lwt.catch
                 (fun () ->
                  (if filter work_id
                   then
                     begin
                       Lwt_log.debug_f
                         "Doing work: id=%li, item=%s"
                         work_id
                         (Work.show work_item) >>= fun () ->

                       self # handle_work_item work_item work_id >>= fun () ->
                       alba_client # mgr_access # mark_work_completed ~work_id
                     end
                   else
                     Lwt.return_unit)
                  >>= fun () ->
                  Lwt.return `Finished)
                 (function
                   | Lwt.Canceled -> Lwt.fail Lwt.Canceled
                   | exn ->
                     Lwt_log.info_f
                       "Got exception %s while working on item %li"
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
                    "Finished work item %li"
                    work_id >>= fun () ->
                  if not (Hashtbl.mem work_threads work_id)
                  then
                    Lwt_log.warning_f
                      "No entry in hashtbl for this workthread %li"
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

    coordinator # add_on_position_changed (fun () -> next_work_item <- 0l);

    let rec inner () =
      alba_client # mgr_access # get_work
                  ~first:next_work_item ~finc:true
                  ~last:None ~max:100 ~reverse:false
      >>= fun ((cnt, work_items), _) ->
      Lwt_log.debug_f "Adding work, got %i items" cnt >>= fun () ->
      self # add_work_threads work_items;

      if once && cnt > 0
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
            ([%show : int32 list]
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
                (fun () -> alba_client # mgr_access # get_namespace ~namespace)
                (fun exn ->  Lwt.return current)
              >>= wait_until_ns_active
         in
         let current = Some (namespace, namespace_info) in
         wait_until_ns_active current >>= fun ns_info ->

         alba_client # nsm_host_access # maybe_update_namespace_info
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
                   "Exception during %s maintenance for %S %li"
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
    let next_id = ref 0l in

    let rec inner () =
      Lwt.catch
        (fun () ->
           alba_client # mgr_access # list_namespaces_by_id
             ~first:!next_id ~finc:true ~last:None
             ~max:100
           >>= fun ((cnt, namespaces), has_more) ->

           List.iter
             (fun (namespace_id, namespace, namespace_info) ->
                next_id := Int32.succ namespace_id;
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
      (alba_client # mgr_access)
      (alba_client # nsm_host_access)
      (alba_client # osd_access)
  end
