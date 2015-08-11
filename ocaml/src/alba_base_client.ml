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
open Lwt
open Remotes
open Checksum
open Slice
open Alba_statistics
open Fragment_cache
open Alba_interval
open Recovery_info
open Alba_client_errors
module Osd_sec = Osd
open Nsm_host_access
open Osd_access

let hm_to_source =  function
  | true  -> Statistics.Cache
  | false -> Statistics.NsmHost

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


let fragment_multiple = Fragment_helper.fragment_multiple


let default_buffer_pool = Buffer_pool.default_buffer_pool

class client
    (fragment_cache : cache)
    ~(mgr_access : Albamgr_client.client)
    ~manifest_cache_size
    ~bad_fragment_callback
    ~nsm_host_connection_pool_size
    ~osd_connection_pool_size
  =

  let nsm_host_access =
    new nsm_host_access
        mgr_access
        nsm_host_connection_pool_size
        default_buffer_pool
  in

  let osd_access =
    new osd_access mgr_access osd_connection_pool_size
  in
  let with_osd_from_pool ~osd_id f = osd_access # with_osd ~osd_id f in
  let osds_info_cache = osd_access # osds_info_cache in
  let get_osd_info = osd_access # get_osd_info in

  let get_namespace_osds_info_cache ~namespace_id =
    nsm_host_access # get_namespace_info ~namespace_id >>= fun (_, osds, _) ->
    osd_access # osds_to_osds_info_cache osds
  in

  let preset_cache = Hashtbl.create 3 in
  let get_preset_info ~preset_name =
    try Lwt.return (Hashtbl.find preset_cache preset_name)
    with Not_found ->
      let get_preset () =
        mgr_access # get_preset ~preset_name >>= fun preset_o ->
        let _name, preset, _is_default, _in_use = Option.get_some preset_o in
        Lwt.return preset
      in
      get_preset () >>= fun preset ->

      if not (Hashtbl.mem preset_cache preset_name)
      then begin
        Lwt.ignore_result begin
          (* start a thread to refresh the preset periodically *)
          let rec inner () =
            Lwt.catch
              (fun () ->
                 Lwt_extra2.sleep_approx 120. >>= fun () ->
                 get_preset () >>= fun preset ->
                 Hashtbl.replace preset_cache preset_name preset;
                 Lwt.return ())
              (fun exn ->
                 Lwt_log.debug_f
                   ~exn
                   "Error in refresh preset loop %S, ignoring"
                   preset_name) >>= fun () ->
            inner ()
          in
          inner ()
        end;
        Hashtbl.replace preset_cache preset_name preset
      end;
      Lwt.return preset
  in
  let deliver_osd_messages ~osd_id =
    Alba_client_message_delivery.deliver_osd_messages
      mgr_access nsm_host_access osd_access
      ~osd_id
  in
  let osd_msg_delivery_threads = Hashtbl.create 3 in
  object(self)

    val manifest_cache = Manifest_cache.ManifestCache.make manifest_cache_size
    method get_manifest_cache : (string, string) Manifest_cache.ManifestCache.t = manifest_cache
    method get_fragment_cache = fragment_cache

    method mgr_access = mgr_access
    method nsm_host_access = nsm_host_access
    method osd_access = osd_access

    method get_preset_info = get_preset_info

    method get_ns_preset_info ~namespace_id =
      let open Albamgr_protocol.Protocol in
      nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
      get_preset_info ~preset_name:ns_info.Namespace.preset_name

    method get_namespace_osds_info_cache = get_namespace_osds_info_cache

    method discover_osds_check_claimed ?check_claimed_delay () : unit Lwt.t =
      Discovery.discovery
        (fun d ->
           Lwt_extra2.ignore_errors
             (fun () -> osd_access # seen ?check_claimed_delay d))

    method with_osd :
      'a. osd_id : Albamgr_protocol.Protocol.Osd.id ->
      (Osd.osd -> 'a Lwt.t) -> 'a Lwt.t =
      fun ~osd_id f ->
        with_osd_from_pool ~osd_id f

    method with_nsm_client' :
      'a. namespace_id : int32 ->
      (Nsm_client.client -> 'a Lwt.t) -> 'a Lwt.t =
      fun ~namespace_id f ->
        nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->
        f nsm

    method with_nsm_client :
      'a. namespace : string ->
      (Nsm_client.client -> 'a Lwt.t) -> 'a Lwt.t = nsm_host_access # with_nsm_client

    method deliver_messages_to_most_osds ~osds ~preset =
      let mvar = Lwt_mvar.create_empty () in

      Lwt.ignore_result begin
        let osds_delivered = ref [] in
        let finished = ref false in

        Lwt_list.iter_p
          (fun (osd_id, (_ : Albamgr_protocol.Protocol.Osd.NamespaceLink.state)) ->
             Lwt_extra2.ignore_errors
               (fun () ->
                  (if Hashtbl.mem osd_msg_delivery_threads osd_id
                   then begin
                     Hashtbl.replace osd_msg_delivery_threads osd_id `Extend;
                     Lwt.return ()
                   end else begin
                     let rec inner f =
                       Hashtbl.replace osd_msg_delivery_threads osd_id `Busy;
                       Lwt.finalize
                         (fun () ->
                            deliver_osd_messages ~osd_id >>=
                            f
                         )
                         (fun () ->
                            match Hashtbl.find osd_msg_delivery_threads osd_id with
                            | `Busy ->
                              Hashtbl.remove osd_msg_delivery_threads osd_id;
                              Lwt.return ()
                            | `Extend ->
                              inner Lwt.return)
                     in
                     inner
                       (fun () ->
                          osds_delivered := osd_id :: !osds_delivered;
                          osd_access # osds_to_osds_info_cache !osds_delivered >>= fun osds_info_cache ->
                          if get_best_policy
                              preset.Albamgr_protocol.Protocol.Preset.policies
                              osds_info_cache = None
                          then Lwt.return ()
                          else begin
                            if not !finished
                            then begin
                              finished := true;
                              Lwt_mvar.put mvar ()
                            end else
                              Lwt.return ()
                          end
                       )
                   end)
               ))
          osds
      end;

      Lwt.choose
        [ Lwt_mvar.take mvar;
          Lwt_unix.sleep 2. ]


    method claim_osd long_id =
      mgr_access # get_alba_id >>= fun alba_id ->

      mgr_access # check_can_claim ~long_id >>= fun () ->

      mgr_access # get_osd_by_long_id ~long_id >>= fun osd_o ->
      let claim_info, osd_info = Option.get_some osd_o in
      (* TODO check claim_info first (eventhough it's not strictly needed) *)

      let update_osd () =
        Pool.Osd.factory osd_buffer_pool osd_info.Albamgr_protocol.Protocol.Osd.kind
        >>= fun (osd, closer) ->
        Lwt.finalize
          (fun () ->
             let module IRK = Osd_keys.AlbaInstanceRegistration in
             let open Slice in
             let next_alba_instance' =
               wrap_string IRK.next_alba_instance
             in
             let no_checksum = Checksum.NoChecksum in
             osd # get_option next_alba_instance'
             >>= function
             | Some _ ->
               osd # get_exn (wrap_string (IRK.instance_log_key 0l))
               >>= fun alba_id' ->
               let u_alba_id' = get_string_unsafe alba_id' in
               if u_alba_id' = alba_id
               then Lwt.return `Continue
               else Lwt.return (`ClaimedBy u_alba_id')
             | None ->
               (* the osd is not yet owned, go and tag it *)

               let id_on_osd = 0l in
               let instance_index_key = IRK.instance_index_key ~alba_id in
               let instance_log_key = IRK.instance_log_key id_on_osd in

               let open Osd in

               osd # apply_sequence
                 [ Assert.none next_alba_instance';
                   Assert.none_string instance_log_key;
                   Assert.none_string instance_index_key; ]
                 [ Update.set
                     next_alba_instance'
                     (wrap_string (serialize Llio.int32_to (Int32.succ id_on_osd)))
                     no_checksum true;
                   Update.set_string
                     instance_log_key
                     alba_id
                     no_checksum true;
                   Update.set_string
                     instance_index_key
                     (serialize Llio.int32_to id_on_osd)
                     no_checksum true; ]
               >>=
                 function
                 | Ok -> Lwt.return `Continue
                 | _  ->
                    begin
                      osd # get_exn (
                            wrap_string (IRK.instance_log_key 0l))
                      >>= fun alba_id'slice ->
                      let alba_id' = get_string_unsafe alba_id'slice in
                      let r =
                        if alba_id' = alba_id
                        then `Continue
                        else `ClaimedBy alba_id'
                      in
                      Lwt.return r
                    end
          )
          (fun () -> closer ())
      in

      Lwt.catch
        update_osd
        (function
          | exn when Networking2.is_connection_failure_exn exn ->
            Lwt_unix.sleep 1.0 >>= fun () ->
            update_osd ()
          | exn -> Lwt.fail exn)
      >>= function
      | `ClaimedBy alba_id' ->
        Lwt_io.printlf "This OSD is already claimed by alba instance %s" alba_id' >>= fun () ->
        Lwt.fail_with "Failed to add OSD because it's already claimed by another alba instance"
      | `Continue ->
         let open Albamgr_protocol.Protocol in
         Lwt.catch
           (fun () -> mgr_access # mark_osd_claimed ~long_id)
           (function
             | Error.Albamgr_exn (Error.Osd_already_claimed, _) ->
                Lwt_log.debug_f
                  "Osd is already claimed on the albamgr, this could be a race with the maintenance process picking this up" >>= fun () ->
                mgr_access # get_osd_by_long_id ~long_id >>= fun r ->
                let claim_info, osd_info = Option.get_some r in
                let open Osd.ClaimInfo in
                (match claim_info with
                 | ThisAlba id -> Lwt.return id
                 | AnotherAlba _
                 | Available ->
                    Lwt_log.debug_f
                      "Osd is already claimed, but strangely enough not by this instance ... consider this a bug" >>= fun () ->
                    Lwt.fail_with "Osd is already claimed, but strangely enough not by this instance ... consider this a bug")
             | exn -> Lwt.fail exn)
        >>= fun osd_id ->
        Lwt_log.debug_f "Claimed osd %s with id=%li" long_id osd_id >>= fun () ->
        (* the statement below is for the side effect of filling the cache *)
        get_osd_info ~osd_id >>= fun _ ->
        Lwt.return osd_id

    method decommission_osd ~long_id =
      mgr_access # get_osd_by_long_id ~long_id >>= fun r ->
      let claim_info, old_osd_info = Option.get_some r in
      let open Albamgr_protocol.Protocol.Osd in

      (match claim_info with
        | ClaimInfo.ThisAlba id ->
          Lwt.return id
        | _ ->
          Lwt.fail_with "can not decommission an osd which isn't yet claimed")
      >>= fun osd_id ->

      Lwt_log.debug_f "old_osd_info:\n%s"
                      ([%show: Albamgr_protocol.Protocol.Osd.t] old_osd_info)
      >>= fun () ->
      Lwt_log.debug_f "updating..." >>= fun () ->
      mgr_access # decommission_osd ~long_id >>= fun () ->
      Hashtbl.remove osds_info_cache osd_id;
      Lwt.return ()

    method get_object_manifest'
        ~namespace_id ~object_name
        ~consistent_read ~should_cache =
      Lwt_log.debug_f
        "get_object_manifest %li %S ~consistent_read:%b ~should_cache:%b"
        namespace_id object_name consistent_read should_cache
      >>= fun () ->
      let lookup_on_nsm_host namespace_id object_name =
        nsm_host_access # get_nsm_by_id ~namespace_id >>= fun client ->
        client # get_object_manifest_by_name object_name
      in
      Manifest_cache.ManifestCache.lookup
        manifest_cache
        namespace_id object_name
        lookup_on_nsm_host
        ~consistent_read ~should_cache


    method upload_packed_fragment_data
             ~namespace_id ~object_id
             ~version_id ~chunk_id ~fragment_id
             ~packed_fragment ~checksum
             ~gc_epoch
             ~recovery_info_slice
             ~osd_id
      =
      let open Osd_keys in
      let set_data =
        Osd.Update.set_string
          (AlbaInstance.fragment
             ~namespace_id
             ~object_id ~version_id
             ~chunk_id ~fragment_id)
          (Lwt_bytes.to_string packed_fragment) checksum false
      in
      let set_recovery_info =
        Osd.Update.set
          (Slice.wrap_string
             (AlbaInstance.fragment_recovery_info
                ~namespace_id
                ~object_id ~version_id
                ~chunk_id ~fragment_id))
          (* TODO do add some checksum *)
          recovery_info_slice Checksum.NoChecksum true
      in
      let set_gc_tag =
        Osd.Update.set_string
          (AlbaInstance.gc_epoch_tag
             ~namespace_id
             ~gc_epoch
             ~object_id ~version_id
             ~chunk_id ~fragment_id)
          "" Checksum.NoChecksum true
      in
      let assert_namespace_active =
        Osd.Assert.value_string
          (AlbaInstance.namespace_status ~namespace_id)
          (Osd.Osd_namespace_state.(serialize
                                      to_buffer
                                      Active)) in

      let do_upload () =
        Lwt_unix.with_timeout
          1.0
          (fun () ->
             with_osd_from_pool
               ~osd_id
               (fun client ->
                  client # apply_sequence
                    [ assert_namespace_active; ]
                    [ set_data;
                      set_recovery_info;
                      set_gc_tag; ]))
      in

      do_upload () >>= fun apply_result ->
      get_osd_info ~osd_id >>= fun (_, state) ->
      match apply_result with
      | Osd_sec.Ok ->
        state.write <- Unix.gettimeofday () :: state.write;
        Lwt.return ()
      | Osd_sec.Exn exn ->
        let open Asd_protocol.Protocol in
        Error.lwt_fail exn


    method upload_chunk
        ~namespace_id
        ~object_id ~object_name
        ~chunk ~chunk_id ~chunk_size
        ~k ~m ~w'
        ~compression ~encryption
        ~fragment_checksum_algo
        ~version_id ~gc_epoch
        ~object_info_o
        ~osds
      =

      let t0 = Unix.gettimeofday () in

      Fragment_helper.chunk_to_packed_fragments
        ~object_id ~chunk_id
        ~chunk ~chunk_size
        ~k ~m ~w'
        ~compression ~encryption ~fragment_checksum_algo
      >>= fun fragments_with_id ->

      let packed_fragment_sizes =
        List.map
          (fun (_, _, (packed_fragment, _, _, _)) ->
           Lwt_bytes.length packed_fragment)
          fragments_with_id
      in
      let fragment_checksums =
        List.map
          (fun (_, _, (_, _, _, checksum)) -> checksum)
          fragments_with_id
      in
      RecoveryInfo.make
        object_name
        object_id
        object_info_o
        encryption
        chunk_size
        packed_fragment_sizes
        fragment_checksums
      >>= fun recovery_info_slice ->


      Lwt_list.map_p
        (fun ((fragment_id,
               fragment,
               (packed_fragment,
                t_compress_encrypt,
                t_hash,
                checksum)),
              osd_id_o) ->
          Statistics.with_timing_lwt
            (fun () ->
               match osd_id_o with
               | None -> Lwt.return ()
               | Some osd_id ->
                 self # upload_packed_fragment_data
                   ~namespace_id
                   ~osd_id
                   ~object_id ~version_id
                   ~chunk_id ~fragment_id
                   ~packed_fragment ~checksum
                   ~gc_epoch
                   ~recovery_info_slice)
          >>= fun (t_store, x) ->

          let t_fragment = Statistics.({
              size_orig = Lwt_bytes.length fragment;
              size_final = Lwt_bytes.length packed_fragment;
              compress_encrypt = t_compress_encrypt;
              hash = t_hash;
              osd_id_o;
              store_osd = t_store;
              total = (Unix.gettimeofday () -. t0)
            }) in

          let res = osd_id_o, checksum in

          Lwt.return (t_fragment, res))
        (List.combine fragments_with_id osds)


    method upload_object_from_file
      ~namespace
      ~object_name
      ~input_file
      ~checksum_o
      ~allow_overwrite
      =
      Lwt_log.debug_f
        "Uploading object %S (namespace=%S) from file %s"
        object_name
        namespace
        input_file >>= fun () ->

      Lwt.catch
        (fun () ->
         Lwt_extra2.with_fd
           input_file
           ~flags:Lwt_unix.([O_RDONLY;])
           ~perm:0o600
           (fun fd ->
              Lwt_unix.fstat fd >>= fun stat ->
              let object_reader = new Object_reader.file_reader fd stat.Lwt_unix.st_size in

              self # upload_object
                ~namespace
                ~object_name
                ~object_reader
                ~checksum_o
                ~allow_overwrite))
        (function
          | Unix.Unix_error(Unix.ENOENT,_,y) ->
             let open Error in failwith FileNotFound
          | (Error.Exn e) as exn ->
             Lwt_log.info_f ~exn "%s" (Error.show e) >>= fun () ->
             Lwt.fail exn
          | exn ->
             Lwt_log.info_f ~exn "generic propagation ..." >>= fun () ->
             Lwt.fail exn
        )

    method upload_object_from_bytes
        ~namespace
        ~(object_name : string)
        ~(object_data : Lwt_bytes.t)
        ~(checksum_o: Checksum.t option)
        ~(allow_overwrite : Nsm_model.overwrite)
      =
      let object_reader = new Object_reader.bytes_reader object_data in

      self # upload_object
        ~namespace
        ~object_name
        ~object_reader
        ~checksum_o
        ~allow_overwrite

    method upload_object_from_string
      ~namespace
      ~object_name
      ~object_data
      ~checksum_o
      ~allow_overwrite =
      self # upload_object
        ~namespace
        ~object_name
        ~object_reader:(new Object_reader.string_reader object_data)
        ~checksum_o
        ~allow_overwrite

    method upload_object_from_string'
      ~namespace_id
      ~object_name
      ~object_data
      ~checksum_o
      ~allow_overwrite =
      self # upload_object'
        ~namespace_id
        ~object_name
        ~object_reader:(new Object_reader.string_reader object_data)
        ~checksum_o
        ~allow_overwrite

    method upload_object
        ~(namespace : string)
        ~(object_name : string)
        ~(object_reader : Object_reader.reader)
        ~(checksum_o: Checksum.t option)
        ~(allow_overwrite : Nsm_model.overwrite)
      =
      nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->
           self # upload_object'
             ~namespace_id
             ~object_name
             ~object_reader
             ~checksum_o
             ~allow_overwrite)

    method upload_object'
      ~namespace_id
      ~object_name
      ~object_reader
      ~checksum_o
      ~allow_overwrite =

      let object_t0 = Unix.gettimeofday () in
      let do_upload timestamp =
        self # upload_object''
          ~object_t0 ~timestamp
          ~object_name
          ~namespace_id
          ~object_reader
          ~checksum_o
          ~allow_overwrite
      in
      Lwt.catch
        (fun () -> do_upload object_t0)
        (fun exn ->
           Lwt_log.debug_f ~exn "Exception while uploading object, retrying once" >>= fun () ->
           let open Nsm_model in
           let timestamp = match exn with
             | Err.Nsm_exn (Err.Old_timestamp, payload) ->
               (* if the upload failed due to the timestamp being not recent
                  enough we should retry with a more recent one...

                  (ideally we should only overwrite the recovery info,
                  so this is a rather brute approach. but for an exceptional
                  situation that's ok.)
               *)
               (deserialize Llio.float_from payload) +. 0.1
             | _ -> object_t0
           in
           begin
             let open Err in
             match exn with
             | Nsm_exn (err, _) ->
               begin match err with
               | Inactive_osd ->
                 Lwt_log.info_f
                   "Upload object %S failed due to inactive (decommissioned) osd, retrying..."
                   object_name >>= fun () ->
                 self # nsm_host_access # refresh_namespace_osds ~namespace_id >>= fun _ ->
                 Lwt.return ()

               | Unknown
               | Old_plugin_version
               | Unknown_operation
               | Inconsistent_read
               | Namespace_id_not_found
               | InvalidVersionId
               | Overwrite_not_allowed
               | Too_many_disks_per_node
               | Insufficient_fragments
               | Object_not_found ->
                 Lwt.fail exn

               | Not_master
               | Old_timestamp
               | Invalid_gc_epoch
               | Invalid_fragment_spread
               | Non_unique_object_id ->
                 Lwt.return ()
               end
             | _ ->
               Lwt.return ()
           end >>= fun () ->
           do_upload timestamp
        )

    method upload_object''
        ~object_t0 ~timestamp
        ~namespace_id
        ~(object_name : string)
        ~(object_reader : Object_reader.reader)
        ~(checksum_o: Checksum.t option)
        ~(allow_overwrite : Nsm_model.overwrite)
      =

      (* TODO
          - retry/error handling/etc where needed
       *)
      (* nice to haves (for performance)
         - upload of multiple chunks could be done in parallel
         - avoid some string copies *)

      object_reader # reset >>= fun () ->

      nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
      let open Albamgr_protocol in
      get_preset_info ~preset_name:ns_info.Protocol.Namespace.preset_name >>= fun preset ->


      nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->

      let policies, w, max_fragment_size,
          compression, fragment_checksum_algo,
          allowed_checksum_algos, verify_upload,
          encryption =
        let open Albamgr_protocol.Protocol.Preset in
        preset.policies, preset.w,
        preset.fragment_size,
        preset.compression, preset.fragment_checksum_algo,
        preset.object_checksum.allowed, preset.object_checksum.verify_upload,
        preset.fragment_encryption
      in
      let w' = Nsm_model.Encoding_scheme.w_as_int w in

      Lwt.catch
        (fun () ->
           get_namespace_osds_info_cache ~namespace_id >>= fun osds_info_cache' ->
           let p =
             get_best_policy_exn
               policies
               osds_info_cache' in
           Lwt.return (p, osds_info_cache'))
        (function
          | Error.Exn Error.NoSatisfiablePolicy ->
            nsm_host_access # refresh_namespace_osds ~namespace_id >>= fun _ ->
            get_namespace_osds_info_cache ~namespace_id >>= fun osds_info_cache' ->
            let p =
              get_best_policy_exn
                policies
                osds_info_cache' in
            Lwt.return (p, osds_info_cache')
          | exn ->
            Lwt.fail exn)
      >>= fun (((k, m, min_fragment_count, max_disks_per_node),
                actual_fragment_count),
               osds_info_cache') ->

      let storage_scheme, encrypt_info =
        let open Nsm_model in
        Storage_scheme.EncodeCompressEncrypt
          (Encoding_scheme.RSVM (k, m, w),
           compression),
        EncryptInfo.from_encryption encryption
      in

      let desired_chunk_size =
        (* TODO this size will often be too big for objects
           that consist of only 1 chunk. object_reader should
           allow getting the total object size in order to not
           waste memory here... *)
        (max_fragment_size / fragment_multiple) * fragment_multiple * k in

      let object_checksum_algo =
        let open Albamgr_protocol.Protocol.Preset in
        match checksum_o with
        | None -> preset.object_checksum.default
        | Some checksum ->
          let checksum_algo = Checksum.algo_of checksum in

          if not (List.mem checksum_algo allowed_checksum_algos)
          then Error.failwith Error.ChecksumAlgoNotAllowed;

          if verify_upload
          then checksum_algo
          else Checksum.Algo.NO_CHECKSUM
      in
      let object_hash = Hashes.make_hash object_checksum_algo in

      let version_id = 0 in

      Lwt_log.debug_f
        "Choosing %i devices from %i candidates for a %i,%i,%i policy"
        actual_fragment_count
        (Hashtbl.length osds_info_cache')
        k m max_disks_per_node
      >>= fun () ->

      let target_devices =
        Choose.choose_devices
          actual_fragment_count
          osds_info_cache' in

      if actual_fragment_count <> List.length target_devices
      then failwith
          (Printf.sprintf
             "Cannot upload object with k=%i,m=%i,actual_fragment_count=%i when only %i active devices could be found for this namespace"
             k m actual_fragment_count (List.length target_devices));

      let target_osds =
        let no_dummies = k + m - actual_fragment_count in
        let dummies = List.map (fun _ -> None) Int.(range 0 no_dummies) in
        List.append
          (List.map (fun (osd_id, _) -> Some osd_id) target_devices)
          dummies
      in

      let object_id = get_random_string 32 in

      let fold_chunks () =

        let chunk = Lwt_bytes.create desired_chunk_size in
        let rec inner acc_chunk_sizes acc_fragments_info total_size chunk_times hash_time chunk_id =
          let t0_chunk = Unix.gettimeofday () in
          Statistics.with_timing_lwt
            (fun () -> object_reader # read desired_chunk_size chunk)
          >>= fun (read_data_time, (chunk_size', has_more)) ->

          let total_size' = total_size + chunk_size' in

          Statistics.with_timing_lwt
            (fun () ->
               object_hash # update_lwt_bytes_detached chunk 0 chunk_size')
          >>= fun (hash_time', ()) ->

          let hash_time' = hash_time +. hash_time' in

          let object_info_o =
            if has_more
            then None
            else Some RecoveryInfo.({
                storage_scheme;
                size = Int64.of_int total_size';
                checksum = object_hash # final ();
                timestamp;
              })
          in

          let chunk_size_with_padding =
            let kf = fragment_multiple * k in
            if chunk_size' mod kf = 0
               || k = 1         (* no padding needed/desired for replication *)
            then chunk_size'
            else begin
              let s = ((chunk_size' / kf) + 1) * kf in
              (* the fill here prevents leaking information in the padding bytes *)
              Lwt_bytes.fill chunk chunk_size' (s - chunk_size') (Char.chr 0);
              s
            end
          in
          let chunk' = Lwt_bytes.proxy chunk 0 chunk_size_with_padding in

          self # upload_chunk
            ~namespace_id
            ~object_id ~object_name
            ~chunk:chunk' ~chunk_size:chunk_size_with_padding
            ~chunk_id
            ~k ~m ~w'
            ~compression ~encryption ~fragment_checksum_algo
            ~version_id ~gc_epoch
            ~object_info_o
            ~osds:target_osds
          >>= fun fragment_info ->

          let t_fragments, fragment_info = List.split fragment_info in

          let acc_chunk_sizes' = (chunk_id, chunk_size_with_padding) :: acc_chunk_sizes in
          let acc_fragments_info' = fragment_info :: acc_fragments_info in

          let t_chunk = Statistics.({
              read_data = read_data_time;
              fragments = t_fragments;
              total = Unix.gettimeofday () -. t0_chunk;
            }) in

          let chunk_times' = t_chunk :: chunk_times in
          if has_more
          then
            inner
              acc_chunk_sizes'
              acc_fragments_info'
              total_size'
              chunk_times'
              hash_time'
              (chunk_id + 1)
          else
            Lwt.return ((acc_chunk_sizes', acc_fragments_info'),
                        total_size',
                        chunk_times',
                        hash_time')
        in
        inner [] [] 0 [] 0. 0 in

      fold_chunks ()
      >>= fun ((chunk_sizes', fragments_info'), size, chunk_times, hash_time) ->

      (* all fragments have been stored
         make a manifest and store it in the namespace manager *)

      let fragments_info = List.rev fragments_info' in
      let locations, fragment_checksums =
        Nsm_model.Layout.split fragments_info in

      let chunk_sizes =
        List.map
          snd
          (List.sort
             (fun (chunk_id1, _) (chunk_id2, _) -> compare chunk_id1 chunk_id2)
             chunk_sizes') in
      let open Nsm_model in
      let object_checksum = object_hash # final () in
      let checksum =
        match checksum_o with
        | None -> object_checksum
        | Some checksum ->
          if verify_upload &&
             checksum <> object_checksum
          then Error.failwith Error.ChecksumMismatch;
          checksum
      in
      let fragment_packed_sizes =
        List.map
          (fun (ut : Statistics.chunk_upload) ->
             List.map
               (fun ft -> ft.Statistics.size_final)
               ut.Statistics.fragments)
          chunk_times
      in
      let fragment_locations =
        Nsm_model.Layout.map
          (fun osd_id -> osd_id, version_id)
          locations
      in
      let manifest =
        Manifest.make
          ~name:object_name
          ~object_id
          ~storage_scheme
          ~encrypt_info
          ~chunk_sizes
          ~checksum
          ~size:(Int64.of_int size)
          ~fragment_locations
          ~fragment_checksums
          ~fragment_packed_sizes
          ~version_id
          ~max_disks_per_node
          ~timestamp
      in
      let store_manifest () =
        nsm_host_access # get_nsm_by_id ~namespace_id >>= fun client ->
        client # put_object
          ~allow_overwrite
          ~manifest
          ~gc_epoch
      in
      Statistics.with_timing_lwt
        (fun () ->
           Lwt.catch
             store_manifest
             (fun exn ->
                Manifest_cache.ManifestCache.remove
                  manifest_cache
                  namespace_id object_name;
                Lwt.fail exn))
      >>= fun (t_store_manifest, old_manifest_o) ->
      (* TODO maybe clean up fragments from old object *)

      Lwt.ignore_result begin
        (* clean up gc tags we left behind on the osds,
           if it fails that's no problem, the gc will
           come and clean it up later *)
        Lwt.catch
          (fun () ->
             Lwt_list.iteri_p
               (fun chunk_id chunk_locs ->
                  Lwt_list.iteri_p
                    (fun fragment_id osd_id_o ->
                       match osd_id_o with
                       | None -> Lwt.return ()
                       | Some osd_id ->
                         with_osd_from_pool ~osd_id
                           (fun osd ->
                              let remove_gc_tag =
                                Osd.Update.delete_string
                                  (Osd_keys.AlbaInstance.gc_epoch_tag
                                     ~namespace_id
                                     ~gc_epoch
                                     ~object_id
                                     ~version_id
                                     ~chunk_id
                                     ~fragment_id)
                              in
                              osd # apply_sequence [] [ remove_gc_tag; ] >>= fun _ ->
                              Lwt.return ()))
                    chunk_locs)
               locations)
          (fun exn -> Lwt_log.debug_f ~exn "Error while cleaning up gc tags")
      end;

      let t_object = Statistics.({
        size;
        hash = hash_time;
        chunks = chunk_times;
        store_manifest = t_store_manifest;
        total = Unix.gettimeofday () -. object_t0;
      }) in

      Lwt_log.debug_f
        ~section:Statistics.section
        "Uploaded object %S with the following timings: %s"
        object_name (Statistics.show_object_upload t_object)
      >>= fun () ->
      let open Manifest_cache in
      ManifestCache.add
        manifest_cache
        namespace_id object_name manifest;

      Lwt.return (manifest, t_object)

    method download_fragment
        ~osd_id_o
        ~namespace_id
        ~object_id ~object_name
        ~chunk_id ~fragment_id
        ~replication
        ~version_id
        ~fragment_checksum
        decompress
        ~encryption =

      (match osd_id_o with
       | None -> Lwt.fail_with "can't download fragment from None osd"
       | Some osd_id -> Lwt.return osd_id)
      >>= fun osd_id ->

      let t0_fragment = Unix.gettimeofday () in

      let key_string =
        Osd_keys.AlbaInstance.fragment
          ~namespace_id
          ~object_id ~version_id
          ~chunk_id ~fragment_id
      in
      let key = Slice.wrap_string key_string in

      let retrieve key =
        fragment_cache # lookup
                       namespace_id key_string
                       read_it
        >>= function
        | None ->
           begin
             Lwt_log.debug_f "fragment not in cache, trying osd:%li" osd_id
             >>= fun () ->

             Lwt.catch
               (fun () ->
                  with_osd_from_pool
                    ~osd_id
                    (fun device_client ->
                       device_client # get_option key))
               (let open Asd_protocol.Protocol in
                function
                | (Error.Exn err) as exn -> begin match err with
                    | Error.Unknown_error _
                    | Error.ProtocolVersionMismatch _ ->
                      bad_fragment_callback
                        self
                        ~namespace_id ~object_id ~object_name
                        ~chunk_id ~fragment_id ~version_id
                    | Error.Full (* a bit silly as this is not an update *)
                    | Error.Assert_failed _
                    | Error.Unknown_operation ->
                      ()
                  end;
                  Lwt.fail exn
                | exn -> Lwt.fail exn)
             >>= function
             | None ->
               let msg =
                 Printf.sprintf
                   "Detected missing fragment namespace_id=%li object_id=%S osd_id=%li (chunk,fragment,version)=(%i,%i,%i)"
                   namespace_id object_id osd_id
                   chunk_id fragment_id version_id
               in
               Lwt_log.warning msg >>= fun () ->
               bad_fragment_callback
                 self
                 ~namespace_id ~object_id ~object_name
                 ~chunk_id ~fragment_id ~version_id;
               (* TODO loopke die queue harvest en nr albamgr duwt *)
               (* TODO testje *)
               Lwt.fail_with msg
             | Some (data:Slice.t) ->
               get_osd_info ~osd_id >>= fun (_, state) ->
               state.read <- Unix.gettimeofday () :: state.read;
               Lwt.ignore_result
                 (fragment_cache # add
                    namespace_id key_string
                    (Slice.get_string_unsafe data)
                 );
               let hit_or_mis = false in
               Lwt.return (hit_or_mis, data)
           end
        | Some data ->
           let hit_or_mis = true in
           Lwt.return (hit_or_mis, Slice.wrap_string data)
      in
      Statistics.with_timing_lwt (fun () -> retrieve key)

      >>= fun (t_retrieve, (hit_or_miss, fragment_data)) ->

      let fragment_data' = Slice.to_bigarray fragment_data in

      Statistics.with_timing_lwt
        (fun () ->
           Fragment_helper.verify fragment_data' fragment_checksum)
      >>= fun (t_verify, checksum_valid) ->

      (if checksum_valid
       then Lwt.return ()
       else begin
        bad_fragment_callback
          self
          ~namespace_id ~object_id ~object_name
          ~chunk_id ~fragment_id ~version_id;
        Lwt.fail_with "Checksum mismatch"
       end) >>= fun () ->

      Statistics.with_timing_lwt
        (fun () ->
           Fragment_helper.maybe_decrypt
             encryption
             ~object_id ~chunk_id ~fragment_id
             ~ignore_fragment_id:replication
             fragment_data')
      >>= fun (t_decrypt, maybe_decrypted) ->

      Statistics.with_timing_lwt
        (fun () -> decompress maybe_decrypted)
      >>= fun (t_decompress, (maybe_decompressed : Lwt_bytes.t)) ->

      let t_fragment = Statistics.({
          osd_id;
          retrieve = t_retrieve;
          hit_or_miss;
          verify = t_verify;
          decrypt = t_decrypt;
          decompress = t_decompress;
          total = Unix.gettimeofday () -. t0_fragment;
        }) in

      Lwt.return (t_fragment, maybe_decompressed)


    method download_chunk
        ~namespace_id
        ~object_id ~object_name
        chunk_locations ~chunk_id
        decompress
        ~encryption
        k m w' =

      let t0_chunk = Unix.gettimeofday () in

      let n = k + m in
      let fragments = Hashtbl.create n in

      let module CountDownLatch = Lwt_extra2.CountDownLatch in
      let successes = CountDownLatch.create ~count:k in
      let failures = CountDownLatch.create ~count:(m+1) in

      let threads : unit Lwt.t list =
        List.mapi
          (fun fragment_id ((osd_id_o, version_id), fragment_checksum) ->
             let t =
               Lwt.catch
                 (fun () ->
                    self # download_fragment
                      ~namespace_id
                      ~osd_id_o
                      ~object_id
                      ~object_name
                      ~chunk_id
                      ~fragment_id
                      ~replication:(k=1)
                      ~version_id
                      ~fragment_checksum
                      decompress
                      ~encryption
                    >>= fun (t_fragment, fragment_data) ->

                    Hashtbl.add fragments fragment_id (fragment_data, t_fragment);
                    CountDownLatch.count_down successes;
                    Lwt.return ())
                 (function
                   | Canceled -> Lwt.return ()
                   | exn ->
                     Lwt_log.debug_f
                       ~exn
                       "Downloading fragment %i failed"
                       fragment_id >>= fun () ->
                     CountDownLatch.count_down failures;
                     Lwt.return ()) in
             Lwt.ignore_result t;
             t)
          chunk_locations in

      ignore threads;

      Lwt.pick [ CountDownLatch.await successes;
                 CountDownLatch.await failures; ] >>= fun () ->

      let () =
        if Hashtbl.length fragments < k
        then
          let () =
            Lwt_log.ign_warning_f
              "could not receive enough fragments for chunk %i; got %i while %i needed\n%!"
              chunk_id (Hashtbl.length fragments) k
          in
          Error.failwith Error.NotEnoughFragments
      in
      let fragment_size =
        let _, (bs, _) = Hashtbl.choose fragments |> Option.get_some in
        Lwt_bytes.length bs
      in

      let rec gather_fragments end_fragment acc_fragments erasures cnt = function
        | fragment_id when fragment_id = end_fragment -> acc_fragments, erasures, cnt
        | fragment_id ->
           let fragment_bigarray, erasures', cnt' =
             if Hashtbl.mem fragments fragment_id
             then fst (Hashtbl.find fragments fragment_id), erasures, cnt + 1
             else Lwt_bytes.create fragment_size, fragment_id :: erasures, cnt in
           if Lwt_bytes.length fragment_bigarray <> fragment_size
           then failwith (Printf.sprintf "fragment %i,%i has size %i while %i expected\n%!" chunk_id fragment_id (Lwt_bytes.length fragment_bigarray) fragment_size);
           gather_fragments
             end_fragment
             (fragment_bigarray :: acc_fragments)
             erasures'
             cnt'
             (fragment_id + 1) in

      let t0_gather_decode = Unix.gettimeofday () in
      let data_fragments_rev, erasures_rev, cnt = gather_fragments k [] [] 0 0 in
      let coding_fragments_rev, erasures_rev', cnt = gather_fragments n [] erasures_rev cnt k in

      let data_fragments = List.rev data_fragments_rev in
      let coding_fragments = List.rev coding_fragments_rev in


      let erasures = List.rev (-1 :: erasures_rev') in

        Lwt_log.ign_debug_f
          "erasures = %s"
          ([%show: int list] erasures);

      Erasure.decode
        ~k ~m ~w:w'
        erasures
        data_fragments
        coding_fragments
        fragment_size >>= fun () ->

      let t_now = Unix.gettimeofday () in

      let t_fragments =
        Hashtbl.fold
          (fun _ (_, t_fragment) acc ->
             t_fragment :: acc)
          fragments
          []
      in

      let t_chunk = Statistics.({
          gather_decode = t_now -. t0_gather_decode;
          total = t_now -. t0_chunk;
          fragments = t_fragments;
        }) in

      Lwt.return (data_fragments, coding_fragments, t_chunk)

    method download_object_slices
      ~namespace
      ~object_name
      ~(object_slices : (Int64.t * int) list)
      ~consistent_read
      write_data =
      nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->
           self # download_object_slices'
             ~namespace_id
             ~object_name
             ~object_slices
             ~consistent_read
             write_data)

    method download_object_slices'
      ~namespace_id
      ~object_name
      ~(object_slices : (Int64.t * int) list)
      ~consistent_read
      write_data =
      self # get_object_manifest'
        ~namespace_id ~object_name
        ~consistent_read ~should_cache:true
      >>= fun (cache_hit, r) ->
      let attempt_download_slices manifest =
         begin
           let open Nsm_model in

           let slices =
             List.fold_left
               (fun (res_offset, acc) (off, len) ->
                (res_offset + len, (res_offset, off, len) :: acc))
               (0, [])
               object_slices |>
               snd |>
               List.sort (fun (_, off1, _) (_, off2, _) -> compare off1 off2) |>
               List.map
                 (fun (res_offset, offset, length) ->
                  (res_offset,
                   Interval.({ offset; length; })))
           in

           let (res_offset, last_slice), length =
             (* ensuring our client doesn't ask stupid things *)
             List.fold_left
               (fun ((_, prev), acc_length) (res_offset, object_slice) ->
                let open Interval in

                if object_slice.length < 0
                then Error.(failwith BadSliceLength);

                if overlap prev object_slice
                then Error.(failwith OverlappingSlices);

                (res_offset, object_slice), Int64.(add acc_length (of_int object_slice.length)))
               ((0, Interval.({ offset = -1L; length = 0; })), 0L)
               slices
           in

           assert (res_offset < 2 lsl 31);

           (* length sanity check (this also filters out an empty slices list) *)
           if length = 0L
           then Lwt.return (Some manifest)
           else if Int64.(manifest.Manifest.size <:
                            add last_slice.Interval.offset (of_int last_slice.Interval.length))
           then Error.(failwith SliceOutsideObject)
           else begin

               let object_id = manifest.Manifest.object_id in
               let es, compression = match manifest.Manifest.storage_scheme with
                 | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
               let enc = manifest.Manifest.encrypt_info in
               let decompress = Fragment_helper.maybe_decompress compression in
               let k, m, w = match es with
                 | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
               let w' = Encoding_scheme.w_as_int w in
               let locations = manifest.Manifest.fragment_locations in
               let fragment_checksums = manifest.Manifest.fragment_checksums in
               let fragment_info =
                 Layout.combine
                   locations
                   fragment_checksums
               in

               let open Albamgr_protocol.Protocol in
               nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
               get_preset_info ~preset_name:ns_info.Namespace.preset_name >>= fun preset ->
               let encryption = Preset.get_encryption preset enc in

               let _, _, intersections_rev =
                 List.fold_left
                   (fun (chunk_offset, chunk_id, acc) chunk_size ->
                    let fragment_size = chunk_size / k in

                    let rec inner acc fragment_offset = function
                      | fragment_id when fragment_id = k -> acc
                      | fragment_id ->
                         let fragment_slice = Interval.({ offset = fragment_offset;
                                                          length = fragment_size; }) in
                         let intersecting_slices =
                           List.fold_left
                             (fun acc (res_offset, slice) ->
                              let open Interval in
                              match intersection fragment_slice slice with
                              | None -> acc
                              | Some fragment_slice_intersection ->
                                 let res_offset' =
                                   res_offset +
                                     Int64.(to_int
                                              (sub
                                                 fragment_slice_intersection.offset
                                                 slice.offset)) in
                                 (res_offset', fragment_slice_intersection) :: acc)
                             []
                             slices
                         in

                         let acc' =
                           if intersecting_slices = []
                           then acc
                           else (fragment_id, fragment_slice, List.rev intersecting_slices) :: acc
                         in

                         inner
                           acc'
                           Int64.(add fragment_offset (of_int fragment_size))
                           (fragment_id + 1)
                    in
                    let chunk_intersections = inner [] chunk_offset 0 in

                    let acc' =
                      if chunk_intersections = []
                      then acc
                      else (chunk_id, chunk_offset, chunk_size, List.rev chunk_intersections) :: acc
                    in

                    (Int64.(add chunk_offset (of_int chunk_size)), chunk_id + 1, acc'))
                   (0L, 0, [])
                   manifest.Manifest.chunk_sizes
               in

               let intersections = List.rev intersections_rev in

               Lwt_list.iter_s
                 (fun (chunk_id, chunk_offset, chunk_size, chunk_intersections) ->
                  let chunk_locations = List.nth_exn fragment_info chunk_id in

                  let relevant_fragments =
                    List.fold_left
                      (fun acc (fragment_id, fragment_slice, fragment_intersections) ->
                       IntMap.add
                         fragment_id
                         (fragment_slice, fragment_intersections)
                         acc)
                      IntMap.empty
                      chunk_intersections
                  in

                  let download_fragments () =
                    Lwt_list.map_p
                      (fun (fragment_id, _, _) ->
                         let (osd_id_o, version_id), fragment_checksum =
                           List.nth_exn chunk_locations fragment_id
                         in
                         self # download_fragment
                           ~namespace_id
                           ~object_id ~object_name
                           ~osd_id_o ~version_id
                           ~chunk_id ~fragment_id
                           ~replication:(k=1)
                           ~fragment_checksum
                           decompress
                           ~encryption
                         >>= fun (t_fragment, fragment_data) ->
                         Lwt.return (fragment_id, fragment_data))
                      chunk_intersections
                  in

                  let condition = Lwt_condition.create () in

                  let download_chunk =
                    Lwt_condition.wait condition >>= function
                    | `FragmentsSucceeded ->
                       Lwt.fail Lwt.Canceled
                    | `FragmentsFailed
                    | `Timeout ->
                       self # download_chunk
                            ~namespace_id
                            ~encryption
                            ~object_id
                            ~object_name
                            chunk_locations ~chunk_id
                            decompress
                            k m w' >>= fun (data_fragments, _, t_chunk) ->
                       let data_fragments_i =
                         List.mapi
                           (fun fragment_id fragment_data ->
                            fragment_id, fragment_data)
                           data_fragments in

                       let relevant_fragments_data =
                         List.filter
                           (fun (fragment_id, _) -> IntMap.mem fragment_id relevant_fragments)
                           data_fragments_i
                       in

                       Lwt.return relevant_fragments_data
                  in

                  Lwt.ignore_result begin
                      Lwt_unix.sleep 1. >>= fun () ->
                      Lwt_condition.signal condition `Timeout;
                      Lwt.return ()
                    end;

                  Lwt.pick [
                      begin
                        Lwt.catch
                          download_fragments
                          (fun exn ->
                           Lwt_condition.signal condition `FragmentsFailed;
                           Lwt.fail exn)
                      end;
                      download_chunk;
                    ]
                  >>= fun fragments_data ->
                  Lwt_condition.signal condition `FragmentsSucceeded;

                  (* write all data for this chunk *)
                  Lwt_list.iter_s
                    (fun (fragment_id, fragment_data) ->
                     let fragment_slice, fragment_intersections =
                       IntMap.find fragment_id relevant_fragments
                     in
                     Lwt_list.iter_s
                       (fun (res_offset, fragment_intersection) ->
                        let open Interval in
                        let off =
                          let open Int64 in
                          to_int (sub fragment_intersection.offset fragment_slice.offset) in

                        write_data
                          res_offset
                          fragment_data
                          off
                          fragment_intersection.length)
                       fragment_intersections)
                    fragments_data
                 )
                 intersections >>= fun () ->

               Lwt.return (Some manifest)
             end
         end
      in
      match r with
      | None -> Lwt.return None
      | Some manifest ->
         Lwt.catch
           (fun () ->attempt_download_slices manifest)
           (fun exn ->
            match exn with
             | Error.Exn Error.NotEnoughFragments ->
                if cache_hit
                then
                  begin
                    self # get_object_manifest' ~namespace_id ~object_name
                         ~consistent_read:true ~should_cache:true
                    >>= fun (_,r) ->
                    (* Option.map_lwt? *)
                    match r with
                    | Some manifest -> attempt_download_slices manifest
                    | None -> Lwt.return None
                  end
                else Lwt.fail exn
             | exn -> Lwt.fail exn
           )



    method download_object_generic''
        ~namespace_id
        ~manifest
        ~get_manifest_dh
        ~t0_object
        ~(write_object_data :
            (Lwt_bytes.t ->
             int ->
             int ->
             unit Lwt.t))
      =
      let open Nsm_model in

      let object_name = manifest.Manifest.name in
      let object_id = manifest.Manifest.object_id in
      let locations = manifest.Manifest.fragment_locations in
      let fragment_checksums = manifest.Manifest.fragment_checksums in
      let fragment_info =
        Layout.combine
          locations
          fragment_checksums
      in
      let es, compression = match manifest.Manifest.storage_scheme with
        | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
      let enc = manifest.Manifest.encrypt_info in
      let decompress = Fragment_helper.maybe_decompress compression in
      let k, m, w = match es with
        | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
      let w' = Encoding_scheme.w_as_int w in

      let open Albamgr_protocol.Protocol in
      nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
      get_preset_info ~preset_name:ns_info.Namespace.preset_name >>= fun preset ->
      let encryption = Preset.get_encryption preset enc in

      let open Manifest in

      let checksum = manifest.checksum in
      let hash2 = Hashes.make_hash (Checksum.algo_of checksum) in
      let object_size = Int64.to_int manifest.Manifest.size in

      Lwt_list.fold_left_s
        (fun (offset, t_chunks, t_write_data, t_verify) (chunk_id, chunk_locations) ->
         let chunk_size =
           List.nth_exn manifest.Manifest.chunk_sizes chunk_id
         in
         let fragment_size = chunk_size / k in

         self # download_chunk
              ~namespace_id
              ~object_id
              ~object_name
              chunk_locations
              ~chunk_id
              ~encryption
              decompress
              k m w'
         >>= fun (data_fragments, coding_fragments, t_chunk) ->


         Lwt_list.fold_left_s
           (fun (offset, t_write_data, t_verify) fragment ->
            let fragment_size' =
              if offset + fragment_size < object_size
              then fragment_size
              else (object_size - offset)
            in
            Statistics.with_timing_lwt
              (fun () ->
               hash2 # update_lwt_bytes_detached fragment 0 fragment_size')
            >>= fun (t_verify', ()) ->

            Statistics.with_timing_lwt
              (fun () ->
               write_object_data fragment 0 fragment_size') >>= fun (t_write_data', ()) ->
            Lwt.return (offset + fragment_size',
                        t_write_data +. t_write_data',
                        t_verify +. t_verify'))
           (offset, t_write_data, t_verify)
           data_fragments
         >>= fun (offset', t_write_data', t_verify') ->
         Lwt.return (offset',
                     t_chunk :: t_chunks,
                     t_write_data',
                     t_verify'))
        (0, [], 0., 0.)
        (List.mapi (fun i fragment_info -> i, fragment_info) fragment_info)
      >>= fun (_, t_chunks, t_write_data, t_verify) ->

      let checksum2 = hash2 # final () in

      let t_object = Statistics.({
                                    get_manifest_dh = get_manifest_dh;
                                    chunks = t_chunks;
                                    verify = t_verify;
                                    write_data = t_write_data;
                                    total = Unix.gettimeofday () -. t0_object;
                     }) in

      Lwt_log.debug_f
        ~section:Statistics.section
        "Download object %s with timings %s"
        object_name (Statistics.show_object_download t_object) >>= fun () ->

      if checksum <> checksum2
      then
        begin
          let c2s = [% show: Checksum.t ] in
          Lwt_log.warning_f "checksum: %s <-> %s"
                            (c2s (manifest.checksum))
                            (c2s checksum2) >>= fun () ->
          let msg = Printf.sprintf "failing checksum for: %s" object_name in
          Lwt.fail_with msg
        end
      else
        let r = Some (manifest, t_object) in
        Lwt.return r

    method delete_object'
             (nsm_client:Nsm_client.client) ~object_name ~may_not_exist =

      let open Nsm_model in
      nsm_client # delete_object
        ~object_name
        ~allow_overwrite:(if may_not_exist
                          then Unconditionally
                          else AnyPrevious)
      >>= function
      | None ->
        Lwt_log.debug_f "no object with name %s could be found\n" object_name
      | Some old_manifest ->
        (* TODO add en-passant deletion of fragments *)
        Lwt_log.debug_f "object with name %s was deleted\n" object_name

  end
