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
open Lwt_bytes2
open Checksum
open Recovery_info
open Alba_statistics
open Alba_client_common
open Alba_client_errors
open Lwt.Infix
module Osd_sec = Osd

let fragment_multiple = Fragment_helper.fragment_multiple

let upload_packed_fragment_data
      (osd_access : Osd_access_type.t)
      ~namespace_id ~object_id
      ~version_id ~chunk_id ~fragment_id
      ~packed_fragment ~checksum
      ~gc_epoch
      ~recovery_info_blob
      ~osd_id
  =
  let open Osd_keys in
  let set_data =
    Osd.Update.set
      (AlbaInstance.fragment
         ~object_id ~version_id
         ~chunk_id ~fragment_id
       |> Slice.wrap_string)
      (Asd_protocol.Blob.Lwt_bytes packed_fragment)
      checksum false
  in
  let set_recovery_info =
    Osd.Update.set
      (Slice.wrap_string
         (AlbaInstance.fragment_recovery_info
            ~object_id ~version_id
            ~chunk_id ~fragment_id))
      (* TODO do add some checksum *)
      recovery_info_blob Checksum.NoChecksum true
  in
  let set_gc_tag =
    Osd.Update.set_string
      (AlbaInstance.gc_epoch_tag
         ~gc_epoch
         ~object_id ~version_id
         ~chunk_id ~fragment_id)
      "" Checksum.NoChecksum true
  in
  let do_upload () =
    let msg = Printf.sprintf "do_upload ~osd_id:%li" osd_id in
    Lwt_extra2.with_timeout ~msg (osd_access # osd_timeout)
      (fun () ->
       osd_access # with_osd
         ~osd_id
         (fun client ->
          (client # namespace_kvs namespace_id) # apply_sequence
                 (osd_access # get_default_osd_priority)
                 []
                 [ set_data;
                   set_recovery_info;
                   set_gc_tag; ]))
  in

  do_upload () >>= fun apply_result ->
  osd_access # get_osd_info ~osd_id >>= fun (_, state, _) ->
  match apply_result with
  | Osd_sec.Ok ->
     Osd_state.add_write state;
     Lwt.return ()
  | Osd_sec.Exn exn ->
     let open Asd_protocol.Protocol in
     Error.lwt_fail exn

let upload_chunk
      osd_access
      ~namespace_id
      ~object_id ~object_name
      ~chunk ~chunk_id ~chunk_size
      ~k ~m ~w'
      ~compression ~encryption
      ~fragment_checksum_algo
      ~version_id ~gc_epoch
      ~object_info_o
      ~osds
      ~(fragment_cache : Fragment_cache.cache)
      ~cache_on_write
  =

  let t0 = Unix.gettimeofday () in

  Fragment_helper.chunk_to_packed_fragments
    ~object_id ~chunk_id
    ~chunk ~chunk_size
    ~k ~m ~w'
    ~compression ~encryption ~fragment_checksum_algo
  >>= fun (unpacked_data_fragments, fragments_with_id) ->

  let t_add_to_fragment_cache =
    if cache_on_write
    then
      Lwt_list.iteri_p
        (fun fragment_id unpacked_data_fragment ->
         let cache_key =
           Fragment_cache_keys.make_key
             ~object_id
             ~chunk_id
             ~fragment_id
         in
         fragment_cache # add
                        namespace_id
                        cache_key
                        unpacked_data_fragment)
        unpacked_data_fragments
    else
      Lwt.return_unit
  in

  Lwt.finalize
    (fun () ->
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
        with_timing_lwt
          (fun () ->
           match osd_id_o with
           | None -> Lwt.return ()
           | Some osd_id ->
              upload_packed_fragment_data
                osd_access
                ~namespace_id
                ~osd_id
                ~object_id ~version_id
                ~chunk_id ~fragment_id
                ~packed_fragment ~checksum
                ~gc_epoch
                ~recovery_info_blob:(Asd_protocol.Blob.Slice recovery_info_slice))
        >>= fun (t_store, x) ->

        let t_fragment = Statistics.({
                                        size_orig = Bigstring_slice.length fragment;
                                        size_final = Lwt_bytes.length packed_fragment;
                                        compress_encrypt = t_compress_encrypt;
                                        hash = t_hash;
                                        osd_id_o;
                                        store_osd = t_store;
                                        total = (Unix.gettimeofday () -. t0)
                                      }) in

        let res = osd_id_o, checksum in

        Lwt.return (t_fragment, res))
       (List.combine fragments_with_id osds) >>= fun r ->
     t_add_to_fragment_cache >>= fun () ->
     Lwt.return r)
    (fun () ->
     t_add_to_fragment_cache >>= fun () ->
     let () =
       if k = 1
       then
         Lwt_bytes.unsafe_destroy
           (List.hd_exn fragments_with_id
            |> fun (_, _, (f, _, _, _)) -> f)
       else
         List.iter
           (fun (_, _, (f, _, _, _)) ->
            Lwt_bytes.unsafe_destroy f)
           fragments_with_id
     in
     Lwt.return_unit)

let upload_object''
      (nsm_host_access : Nsm_host_access.nsm_host_access)
      osd_access
      get_preset_info
      get_namespace_osds_info_cache
      ~object_t0 ~timestamp
      ~namespace_id
      ~(object_name : string)
      ~(object_reader : Object_reader.reader)
      ~(checksum_o: Checksum.t option)
      ~(object_id_hint: string option)
      ~fragment_cache
      ~cache_on_write
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
            actual_fragment_count, _),
           osds_info_cache') ->

  let storage_scheme, encrypt_info =
    let open Nsm_model in
    Storage_scheme.EncodeCompressEncrypt
      (Encoding_scheme.RSVM (k, m, w),
       compression),
    Encrypt_info_helper.from_encryption encryption
  in

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

  let object_id =
    match object_id_hint with
    | None -> get_random_string 32
    | Some hint -> hint
  in

  object_reader # length >>= fun object_length ->

  let desired_chunk_size =
    let x = fragment_multiple * k in
    if max_fragment_size > (object_length / k)
    then ((object_length / x) + 1) * x
    else (max_fragment_size / fragment_multiple) * x
  in

  let fold_chunks chunk =

    let rec inner acc_chunk_sizes acc_fragments_info total_size chunk_times hash_time chunk_id =
      let t0_chunk = Unix.gettimeofday () in
      let chunk_size' = min desired_chunk_size (object_length - total_size) in
      Lwt_log.debug_f "chunk_size' = %i" chunk_size' >>= fun () ->
      with_timing_lwt
        (fun () -> object_reader # read chunk_size' chunk)
      >>= fun (read_data_time, ()) ->

      let total_size' = total_size + chunk_size' in
      let has_more = total_size' < object_length in

      with_timing_lwt
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
      let chunk' = Lwt_bytes.extract chunk 0 chunk_size_with_padding in

      Lwt.finalize
        (fun () ->
         upload_chunk
           osd_access
           ~namespace_id
           ~object_id ~object_name
           ~chunk:chunk' ~chunk_size:chunk_size_with_padding
           ~chunk_id
           ~k ~m ~w'
           ~compression ~encryption ~fragment_checksum_algo
           ~version_id ~gc_epoch
           ~object_info_o
           ~osds:target_osds
           ~fragment_cache
           ~cache_on_write)
        (fun () ->
         Lwt_bytes.unsafe_destroy chunk';
         Lwt.return ())
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
        Lwt.return ((List.rev acc_chunk_sizes',
                     List.rev acc_fragments_info'),
                    total_size',
                    List.rev chunk_times',
                    hash_time')
    in
    inner [] [] 0 [] 0. 0 in

  let chunk = Lwt_bytes.create desired_chunk_size in
  Lwt.finalize
    (fun () -> fold_chunks chunk)
    (fun () -> Lwt_bytes.unsafe_destroy chunk;
               Lwt.return ())
  >>= fun ((chunk_sizes', fragments_info), size, chunk_times, hash_time) ->

  (* all fragments have been stored
         make a manifest and store it in the namespace manager *)

  let locations, fragment_checksums =
    Nsm_model.Layout.split fragments_info in

  let chunk_sizes = List.map snd chunk_sizes' in
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
  let almost_t_object t_store_manifest =
    Statistics.({
                   size;
                   hash = hash_time;
                   chunks = chunk_times;
                   store_manifest = t_store_manifest;
                   total = Unix.gettimeofday () -. object_t0;
                 }) in
  Lwt.return (manifest, almost_t_object, gc_epoch)

let cleanup_gc_tags
      (osd_access : Osd_access_type.t)
      mf
      gc_epoch
      ~namespace_id
  =
  (* clean up gc tags we left behind on the osds,
   * if it fails that's no problem, the gc will
   * come and clean it up later *)
  Lwt.catch
    (fun () ->
     Lwt_list.iteri_p
       (fun chunk_id chunk_locs ->
        Lwt_list.iteri_p
          (fun fragment_id (osd_id_o, version_id) ->
           match osd_id_o with
           | None -> Lwt.return ()
           | Some osd_id ->
              osd_access # with_osd
                         ~osd_id
                         (fun osd ->
                          let remove_gc_tag =
                            Osd.Update.delete_string
                              (Osd_keys.AlbaInstance.gc_epoch_tag
                                 ~gc_epoch
                                 ~object_id:mf.Nsm_model.Manifest.object_id
                                 ~version_id
                                 ~chunk_id
                                 ~fragment_id)
                          in
                          (osd # namespace_kvs namespace_id) # apply_sequence
                                                             (osd_access # get_default_osd_priority)
                                                             [] [ remove_gc_tag; ] >>= fun _ ->
                          Lwt.return ()))
          chunk_locs)
       mf.Nsm_model.Manifest.fragment_locations)
    (fun exn -> Lwt_log.debug_f ~exn "Error while cleaning up gc tags")
  |> Lwt.ignore_result


let store_manifest
      (nsm_host_access : Nsm_host_access.nsm_host_access)
      (osd_access : Osd_access_type.t)
      manifest_cache
      ~namespace_id
      ~allow_overwrite
      (manifest, almost_t_object, gc_epoch) =
  let object_name = manifest.Nsm_model.Manifest.name in
  let store_manifest () =
    nsm_host_access # get_nsm_by_id ~namespace_id >>= fun client ->
    client # put_object
           ~allow_overwrite
           ~manifest
           ~gc_epoch
  in
  with_timing_lwt
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

  let () = cleanup_gc_tags osd_access manifest gc_epoch ~namespace_id in

  let t_object = almost_t_object t_store_manifest in

  Lwt_log.debug_f
    ~section:Statistics.section
    "Uploaded object %S with the following timings: %s"
    object_name (Statistics.show_object_upload t_object)
  >>= fun () ->
  let open Manifest_cache in
  ManifestCache.add
    manifest_cache
    namespace_id object_name manifest;

  Lwt.return (manifest, t_object, namespace_id)

let upload_object'
      nsm_host_access osd_access
      manifest_cache
      get_preset_info
      get_namespace_osds_info_cache
      ~namespace_id
      ~object_name
      ~object_reader
      ~checksum_o
      ~allow_overwrite
      ~object_id_hint
      ~fragment_cache
      ~cache_on_write
  =

  let object_t0 = Unix.gettimeofday () in
  let do_upload timestamp =
    upload_object''
      nsm_host_access
      osd_access
      get_preset_info
      get_namespace_osds_info_cache
      ~object_t0 ~timestamp
      ~object_name
      ~namespace_id
      ~object_reader
      ~checksum_o
      ~object_id_hint
      ~fragment_cache
      ~cache_on_write
    >>=
      store_manifest
        nsm_host_access
        osd_access
        manifest_cache
        ~namespace_id
        ~allow_overwrite
  in
  Lwt.catch
    (fun () -> do_upload object_t0)
    (fun exn ->
     Lwt_log.debug_f ~exn "Exception while uploading object, retrying once" >>= fun () ->
     let open Nsm_model in
     let timestamp = match exn with
       | Err.Nsm_exn (Err.Old_timestamp, payload) ->
          (* if the upload failed due to the timestamp being not
             recent enough we should retry with a more recent one...

             (ideally we should only overwrite the recovery info,
             so this is a rather brute approach. but for an
             exceptional situation that's ok.)
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
                   nsm_host_access # refresh_namespace_osds ~namespace_id >>= fun _ ->
                   Lwt.return ()

                | Unknown
                | Old_plugin_version
                | Unknown_operation
                | Inconsistent_read
                | Namespace_id_not_found
                | InvalidVersionId
                | Overwrite_not_allowed
                | Assert_failed
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
