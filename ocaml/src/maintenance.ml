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

open Prelude
open Slice
open Lwt_bytes2
open Recovery_info
open Lwt.Infix


let gc_grace_period = Nsm_host_access.gc_grace_period
type namespace_id = Albamgr_protocol.Protocol.Namespace.id

type flavour =
  | ALL_IN_ONE
  | NO_REBALANCE
  | ONLY_REBALANCE
  [@@deriving show]

class client ?(filter: namespace_id -> bool = fun _ -> true)
             ?(retry_timeout = 60.)
             ?(flavour = ALL_IN_ONE)
             (alba_client : Alba_base_client.client)

  =

  object(self)

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
        Albamgr_protocol.Protocol.Preset.get_encryption preset enc
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
         let key_string =
           Osd_keys.AlbaInstance.fragment
             ~namespace_id
             ~object_id ~version_id:version
             ~chunk_id ~fragment_id
         in
         let key = Slice.wrap_string key_string in
         let open Alba_client_errors.Error in
         Lwt_log.debug_f
           "chunk %i: fetching fragment %i from osd_id:%li"
           chunk_id fragment_id source_osd
         >>= fun () ->

         alba_client # with_osd
           ~osd_id:source_osd
           (fun osd_client -> osd_client # get_option key)
         >>= function
         | None -> Lwt.fail (Exn NotEnoughFragments)
         | Some packed_fragment ->
            Fragment_helper.verify' packed_fragment fragment_checksum
            >>= fun checksum_valid ->
            if not checksum_valid
            then Lwt.fail (Exn ChecksumMismatch)
            else
              begin
                Alba_client_upload.upload_packed_fragment_data
                  (alba_client # osd_access)
                  ~namespace_id ~object_id
                  ~chunk_id ~fragment_id ~version_id:version_id1
                  ~packed_fragment ~checksum:fragment_checksum
                  ~gc_epoch
                  ~recovery_info_slice
                  ~osd_id:target_osd
                >>= fun () ->
                Lwt.return (chunk_id, fragment_id, source_osd, target_osd)
              end
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
            osd_client # apply_sequence [] [Osd.Update.delete_string key]
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
      Lwt.return object_location_movements


    method repair_object_generic
             ~namespace_id
             ~manifest
             ~problem_osds
             ~problem_fragments
      =
      let open Nsm_model in
      Lwt_log.debug_f "_repair_object_generic ~namespace_id:%li ~manifest:%s ~problem_osds:%s ~problem_fragments:%s"
                      namespace_id (Manifest.show manifest)
                      ([%show : int32 list] (Int32Set.elements problem_osds))
                      ([%show : (int * int) list] problem_fragments)
      >>= fun () ->

      let object_name = manifest.Manifest.name in
      let object_id = manifest.Manifest.object_id in

      let locations = manifest.Manifest.fragment_locations in
      let fragment_checksums = manifest.Manifest.fragment_checksums in
      let fragment_info =
        Layout.combine
          locations
          fragment_checksums
      in

      alba_client # get_ns_preset_info ~namespace_id >>= fun preset ->
      alba_client # nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->

      let fragment_checksum_algo =
        preset.Albamgr_protocol.Protocol.Preset.fragment_checksum_algo in

      let es, compression = match manifest.Manifest.storage_scheme with
        | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
      let enc = manifest.Manifest.encrypt_info in
      let encryption = Albamgr_protocol.Protocol.Preset.get_encryption preset enc in
      let decompress = Fragment_helper.maybe_decompress compression in
      let k, m, w = match es with
        | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
      let w' = Encoding_scheme.w_as_int w in

      let version_id = manifest.Manifest.version_id + 1 in

      Lwt_list.map_s
        (fun (chunk_id, chunk_location) ->

           alba_client # get_namespace_osds_info_cache ~namespace_id >>= fun osds_info_cache' ->

           let _, ok_fragments, fragments_to_be_repaired =
             List.fold_left
               (fun (fragment_id, ok_fragments, to_be_repaireds)
                 ((fragment_osd_id_o, fragment_version_id), fragment_checksum) ->
                 let ok_fragments', to_be_repaireds' =
                   if List.mem (chunk_id, fragment_id) problem_fragments ||
                      (match fragment_osd_id_o with
                       | None -> false
                       | Some osd_id -> Int32Set.mem osd_id problem_osds)
                   then
                     ok_fragments,
                     (fragment_id, fragment_checksum) :: to_be_repaireds
                   else
                     fragment_osd_id_o :: ok_fragments,
                     to_be_repaireds in
                 fragment_id + 1, ok_fragments', to_be_repaireds')
               (0, [], [])
               chunk_location in
           if fragments_to_be_repaired = []
           then Lwt.return (chunk_id, [])
           else begin
             alba_client # download_chunk
               ~namespace_id
               ~object_id
               ~object_name
               chunk_location
               ~chunk_id
               ~encryption
               decompress
               k m w'
             >>= fun (data_fragments, coding_fragments, t_chunk) ->

             let all_fragments = List.append data_fragments coding_fragments in
             Lwt.finalize
               (fun () ->
                Maintenance_helper.upload_missing_fragments
                  alba_client
                  osds_info_cache'
                  ok_fragments
                  all_fragments
                  fragments_to_be_repaired
                  ~namespace_id
                  manifest
                  ~chunk_id ~version_id ~gc_epoch
                  locations
                  compression encryption
                  fragment_checksum_algo
                  ~is_replication:(k=1))
               (fun () ->
                List.iter
                  Lwt_bytes.unsafe_destroy
                  all_fragments;
                Lwt.return ())
             >>= fun updated_locations ->

             Lwt.return (chunk_id, updated_locations)
           end)
        (List.mapi (fun i lc -> i, lc) fragment_info)
      >>= fun updated_locations ->

      let updated_object_locations =
        List.fold_left
          (fun acc (chunk_id, updated_locations) ->
             let updated_chunk_locations =
               List.map
                 (fun (fragment_id, device_id) ->
                    (chunk_id, fragment_id, device_id))
                 updated_locations in
             List.rev_append updated_chunk_locations acc)
          []
          updated_locations
      in

      Lwt.return (updated_object_locations, gc_epoch, version_id)

    method private _repair_object_generic
      ~namespace_id
      ~manifest
      ~problem_osds
      ~problem_fragments =
      self # repair_object_generic
        ~namespace_id
        ~manifest
        ~problem_osds
        ~problem_fragments
      >>= fun (updated_object_locations, gc_epoch, version_id) ->

      let open Nsm_model in
      let object_name = manifest.Manifest.name in
      let object_id = manifest.Manifest.object_id in

      alba_client # with_nsm_client'
        ~namespace_id
        (fun client ->
           client # update_manifest
             ~object_name
             ~object_id
             (List.map
                (fun (c,f,o) -> c,f,Some o)
                updated_object_locations)
             ~gc_epoch ~version_id)
      >>= fun () ->
      Lwt_log.debug_f
        "updated_manifest ~namespace_id:%li ~object_id:%S ~updated_object_locations:%s"
        namespace_id object_id
        ([%show : (int *int * int32) list] updated_object_locations)
      >>= fun () ->
      Lwt.return ()

    method decommission_device
        ~namespace_id
        ~osd_id
      =
      Lwt_log.debug_f "Decommissioning osd %li" osd_id >>= fun () ->

      alba_client # with_nsm_client'
        ~namespace_id
        (fun client ->
           client # list_device_objects
             ~osd_id
             ~first:"" ~finc:true ~last:None
             ~max:100 ~reverse:false)
      >>= fun ((cnt, manifests), has_more) ->

      Lwt_list.iter_s
        (fun manifest ->
         Lwt.catch
           (fun () ->
            self # _repair_object_generic
                 ~namespace_id
                 ~manifest
                 ~problem_fragments:[]
                 ~problem_osds:(Int32Set.of_list [ osd_id ]))
           (fun exn ->
            let open Nsm_model.Manifest in
            Lwt_log.info_f
              ~exn
              "Exn while repairing osd %li (object name,id = %s,%s), will now try object rewrite"
              osd_id manifest.name manifest.object_id >>= fun () ->
            Lwt_extra2.ignore_errors
              ~logging:true
              (fun () ->
               self # repair_object_rewrite
                    ~namespace_id
                    ~manifest))
        )
        manifests >>= fun () ->

      if has_more
      then self # decommission_device ~namespace_id ~osd_id
      else Lwt.return ()

    method repair_osd ~osd_id =
      alba_client # mgr_access # list_all_osd_namespaces ~osd_id >>= fun (_, namespaces) ->

      Lwt_list.iter_p
        (fun namespace_id ->
           Lwt_extra2.ignore_errors
             (fun () ->
                self # decommission_device
                  ~namespace_id
                  ~osd_id))
        namespaces

    method repair_osds : unit Lwt.t =
      let threads = Hashtbl.create 3 in
      let rec inner () =
        alba_client # mgr_access # list_all_decommissioning_osds >>= fun (_, osds) ->

        List.iter
          (fun (osd_id, _) ->
             if not (Hashtbl.mem threads osd_id)
             then begin
               Hashtbl.add threads osd_id ();
               Lwt.async
                 (fun () ->
                    Lwt_extra2.ignore_errors
                      (fun () -> self # repair_osd ~osd_id) >>= fun () ->
                    Hashtbl.remove threads osd_id;
                    Lwt.return ())
             end)
          osds;

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

        alba_client # with_osd
          ~osd_id
          (fun osd_client ->
             osd_client # apply_sequence [] upds >>= function
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
               ~keys:keys_to_be_deleted) >>= fun () ->

        if has_more
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
            (fun osd_id -> self # clean_device_obsolete_keys ~namespace_id ~osd_id)
            devices
        end
      else
        begin
          let threads = Hashtbl.create 3 in
          let rec inner () =
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

            Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
            inner ()
          in
          inner ()
        end

    method garbage_collect_device ~osd_id ~namespace_id ~gc_epoch =
      let open Osd_keys in
      let open Slice in
      let first = wrap_string (AlbaInstance.gc_epoch_tag
          ~namespace_id ~gc_epoch:0L
          ~object_id:"" ~version_id:0
          ~chunk_id:0 ~fragment_id:0) in
      let finc = true in
      let last =
        let l = AlbaInstance.gc_epoch_tag
            ~namespace_id ~gc_epoch:(Int64.succ gc_epoch)
            ~object_id:"" ~version_id:0
            ~chunk_id:0 ~fragment_id:0 in
        Some (wrap_string l, false) in
      let rec inner () =
        alba_client # with_osd
          ~osd_id
          (fun client ->
             client # range ~first ~finc ~last ~reverse:false ~max:100)
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
                    client # apply_sequence [] [ Osd.Update.delete gc_tag_key; ]
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
                   Osd.Assert.value gc_tag_key (wrap_string ""); ] in

               Lwt_log.debug_f
                 "Cleaning up garbage fragment ns_id=%li, gc_epoch=%Li, object_id=%S, %i,%i,%i"
                 namespace_id gc_epoch object_id chunk_id fragment_id version_id >>= fun () ->

               alba_client # with_osd
                 ~osd_id
                 (fun client ->
                    client # apply_sequence asserts upds >>= fun _succes ->
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
        if has_more
        then inner ()
        else Lwt.return ()
      in
      inner ()

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
              Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
              inner ()
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

          self # _repair_object_generic
               ~namespace_id
               ~manifest
               ~problem_fragments
               ~problem_osds
        end
      else
        self # repair_object_rewrite
             ~namespace_id
             ~manifest

    method repair_object_rewrite
             ~namespace_id ~manifest =
      let open Nsm_model in

      Lwt_log.debug_f
        "Repairing %s in namespace %li"
        (Manifest.show manifest) namespace_id
      >>= fun () ->

      (* TODO don't download to a string
         instead download to a file without syncing it?
         or download to a bigarray?
      *)
      let object_data = Bytes.create (Int64.to_int manifest.Manifest.size) in
      let offset = ref 0 in
      let write_object_data source pos len =
        Lwt_bytes.blit_to_bytes source pos object_data !offset len;
        offset := !offset + len;
        Lwt.return ()
      in

      alba_client # download_object_generic''
        ~namespace_id
        ~manifest
        ~get_manifest_dh:(0.,Alba_statistics.Statistics.NsmHost)
        ~t0_object:(Unix.gettimeofday ())
        ~write_object_data >>= fun _ ->

      let object_name = manifest.Manifest.name in
      Lwt_log.debug_f
        "Downloaded object %S (%li), reuploading it with changed policy"
        object_name namespace_id >>= fun () ->

      let checksum_o = Some manifest.Manifest.checksum in

      let allow_overwrite = PreviousObjectId manifest.Manifest.object_id in

      (* TODO due to changed circumstances this may actually rewrite the object
         with a worse policy than before! *)
      alba_client # upload_object'
        ~namespace_id
        ~object_name
        ~object_reader:(new Object_reader.string_reader object_data)
        ~checksum_o
        ~allow_overwrite >>= fun _ ->

      Lwt.return ()

    method rebalance_namespace
             ?(delay=0.)
             ?(categorize = Rebalancing_helper.categorize)
             ?(only_once = false)
             ~make_first
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
             ~make_first
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
           Lwt_list.iter_s execute_move best
           >>= fun () ->
           let delay =
             if !migrated
             then
               0. (* go on *)
             else
               let delay' = delay *. 1.5 in
               min 60. (max delay' 1.)
           in
           if only_once then Lwt.return ()
           else
             begin
               Lwt_unix.sleep delay >>= fun () ->
               self # rebalance_namespace
                    ~delay
                    ~categorize
                    ~make_first
                    ~namespace_id ()
             end
         end

    method repair_by_policy_namespace ~namespace_id =

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

            Lwt_list.iter_s
              (fun manifest ->
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
                   ~problem_fragments)
              objs >>= fun () ->

            Lwt.return (cnt > 0)
          end
          else begin
            nsm # list_objects_by_policy ~k ~m ~max:200 >>= fun ((cnt, objs), _) ->
            if cnt > 0
            then
              Lwt_list.iter_s
                (fun manifest -> self # repair_object_rewrite ~namespace_id ~manifest)
                objs >>= fun () ->
              Lwt.return true
            else
              inner tl
          end
      in

      inner policies_rev >>= fun repaired_some ->

      if repaired_some
      then self # repair_by_policy_namespace ~namespace_id
      else Lwt.return ()


    method handle_work_item work_item =
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
            Lwt_extra2.sleep_approx retry_timeout >>=
            inner
          | true ->
            Lwt.return ()
        in
        inner ()
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
          else inner () in
        inner ()
      | CleanupOsdNamespace (osd_id, namespace_id) ->
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
             let rec inner () =
               client # range
                 ~first:namespace_prefix' ~finc:true
                 ~last:namespace_next_prefix
                 ~max:200 ~reverse:false >>= fun ((cnt, keys), has_more) ->
               client # apply_sequence
                 []
                 (List.map
                    (fun k -> Osd.Update.delete k)
                    keys) >>= fun _ ->
               if has_more
               then inner ()
               else Lwt.return ()
             in
             inner ())
      | CleanupNamespaceOsd (namespace_id, osd_id) ->
        Lwt.catch
          (fun () ->
             alba_client # with_nsm_client'
               ~namespace_id
               (fun nsm ->
                  let rec inner () =
                    nsm # cleanup_osd_keys_to_be_deleted osd_id >>= fun cnt ->
                    if cnt <> 0
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
                    Lwt_log.debug_f
                      "Doing work: id=%li, item=%s"
                      work_id
                      (Work.show work_item) >>= fun () ->

                    self # handle_work_item work_item >>= fun () ->
                    alba_client # mgr_access # mark_work_completed ~work_id
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
                 inner (backoff *. 1.5)
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


  method do_work ?(once = false) () : unit Lwt.t =
    let rec inner () =
      alba_client # mgr_access # get_work
        ~first:next_work_item ~finc:true
        ~last:None ~max:100 ~reverse:false
      >>= fun ((cnt, work_items), _) ->
      Lwt_log.debug_f "Adding work, got %i items\n" cnt >>= fun () ->
      self # add_work_threads work_items;

      if once
      then
        if cnt = 0
        then Lwt.return ()
        else inner ()
      else begin
        Lwt_extra2.sleep_approx retry_timeout >>= fun () ->
        inner ()
      end
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
         let core_tasks =
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
             ]
         in
         let rebalance =
           "rebalance",
           fun () -> self # rebalance_namespace
                          ~make_first:(fun () ->get_random_string 32)
                          ~namespace_id ()
         in
         let tasks =
           match flavour with
           |ALL_IN_ONE     -> rebalance :: core_tasks
           |NO_REBALANCE   -> core_tasks
           |ONLY_REBALANCE -> [rebalance]
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
             ~max:(-1)
           >>= fun ((cnt, namespaces), has_more) ->

           List.iter
             (fun (namespace_id, namespace, namespace_info) ->
                next_id := Int32.succ namespace_id;
                if filter namespace_id
                then
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

  method deliver_all_messages () : unit Lwt.t =
    Alba_client_message_delivery.deliver_all_messages
      (alba_client # mgr_access)
      (alba_client # nsm_host_access)
      (alba_client # osd_access)
  end
