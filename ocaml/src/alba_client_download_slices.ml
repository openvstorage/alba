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
open Alba_interval
open Alba_client_errors
open Slice
open Lwt_bytes2
open Lwt.Infix

exception Unreachable_fragment of { chunk_id : Nsm_model.chunk_id;
                                    fragment_id : Nsm_model.fragment_id; }

let blit_from_fragment fragment_data fragment_intersections =
  List.iter
    (fun (offset, length,
          dest, dest_off) ->
      Lwt_bytes.blit
        fragment_data
        offset
        dest
        dest_off
        length)
    fragment_intersections

let try_get_from_fragments
      (osd_access : Osd_access_type.t)
      ~chunk_id
      ~k
      chunk_locations
      chunk_intersections
      ~namespace_id
      ~object_name ~object_id
      ~cache_on_read
      fragment_cache
      compression decompress
      encryption
      fragment_statistics_cb
      ~partial_osd_read
      bad_fragment_callback
  =
  let mfs = ref [] in

  let fetch_fragment fragment_id =
    let location, fragment_checksum =
      List.nth_exn chunk_locations fragment_id
    in
    Lwt.catch
      (fun () ->
        Alba_client_download.download_fragment'
          osd_access
          ~namespace_id
          ~object_id ~object_name
          ~location
          ~chunk_id ~fragment_id
          ~k
          ~fragment_checksum
          decompress
          ~encryption
          ~cache_on_read
          fragment_cache
          bad_fragment_callback)
      (fun exn ->
        Lwt_log.debug_f
          ~exn
          "Exception while downloading fragment namespace_id=%Li object_name,id=(%S,%S) chunk,fragment=(%i,%i) location=%s"
          namespace_id
          object_name object_id
          chunk_id fragment_id
          ([%show : int64 option * int] location) >>= fun () ->
        Lwt.fail (Unreachable_fragment { chunk_id; fragment_id; }))
    >>= fun (t_fragment, fragment_data, mfs') ->
    mfs := List.rev_append mfs' !mfs;
    Lwt.catch
      (fun () ->
        let () = fragment_statistics_cb t_fragment in
        Lwt.return fragment_data)
      (fun exn ->
        Lwt_bytes.unsafe_destroy fragment_data;
        Lwt.fail exn)
  in

  let try_partial_osd_read fragment_id fragment_intersections =
    if partial_osd_read
       && (let open Nsm_model.Compression in
           match compression with
           | NoCompression -> true
           | Snappy | Bzip2 -> false)
       && (let open Encryption.Encryption in
           match encryption with
           | NoEncryption -> true
           | AlgoWithKey (AES (CTR, _), _) -> true
           | AlgoWithKey (AES (CBC, _), _) -> false)
    then
      begin
        let (osd_id_o, version_id), fragment_checksum =
          List.nth_exn chunk_locations fragment_id
        in
        match osd_id_o with
        | None ->
           Lwt.fail (Unreachable_fragment { chunk_id; fragment_id; })
        | Some osd_id ->
           Lwt.catch
             (fun () ->
               osd_access
                 # with_osd
                 ~osd_id
                 (fun osd ->
                   let osd_key =
                     Osd_keys.AlbaInstance.fragment
                       ~object_id ~version_id
                       ~chunk_id ~fragment_id
                     |> Slice.wrap_string
                   in
                   (osd # namespace_kvs namespace_id)
                     # partial_get
                     (osd_access # get_default_osd_priority)
                     osd_key
                     fragment_intersections))
             (fun exn -> Lwt.fail (Unreachable_fragment { chunk_id; fragment_id; }))
           >>=
             let open Osd in
             function
             | Unsupported -> Lwt.return_false
             | Success ->
                Lwt_list.iter_p
                  (fun (fragment_offset, length, buf, buf_offset) ->
                    Fragment_helper.maybe_partial_decrypt
                      encryption
                      ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id:(k=1)
                      (buf, buf_offset, length) ~fragment_offset)
                  fragment_intersections >>= fun () ->
                Lwt.return_true
             | NotFound -> Lwt.fail (Unreachable_fragment { chunk_id; fragment_id; })
      end
    else
      Lwt.return_false
  in

  let get_from_fragments () =
    Lwt_list.iter_p
      (fun (fragment_id, fragment_slice, fragment_intersections) ->
        fragment_cache # lookup2
                       namespace_id
                       (Fragment_cache_keys.make_key
                          ~object_id ~chunk_id ~fragment_id)
                       fragment_intersections
        >>= function
        | (true, mfs') ->
           (* read fragment pieces from the fragment cache *)
           mfs := List.rev_append mfs' !mfs;
           Lwt.return_unit
        | (false, _) ->
           try_partial_osd_read fragment_id fragment_intersections
           >>= function
           | true -> Lwt.return_unit
           | false ->
              (* partial osd read was not possible or desired,
               * try reading entire fragment and blit from that *)
              fetch_fragment fragment_id >>= fun fragment_data ->
              let () =
                finalize
                  (fun () -> blit_from_fragment fragment_data fragment_intersections)
                  (fun () -> Lwt_bytes.unsafe_destroy fragment_data)
              in
              Lwt.return_unit)
      chunk_intersections
  in
  get_from_fragments () >>= fun () ->
  Lwt.return !mfs


let _download_object_slices
      (nsm_host_access : Nsm_host_access.nsm_host_access)
      get_preset_info
      ~namespace_id
      ~manifest
      ~(object_slices : (Int64.t * int * Lwt_bytes.t * int) list)
      ~fragment_statistics_cb
      osd_access
      fragment_cache
      ~cache_on_read
      bad_fragment_callback
      ~partial_osd_read
  =
  let open Nsm_model in

  let object_name = manifest.Manifest.name in

  let slices =
    List.sort
      (fun (off1, _, _, _) (off2, _, _, _) -> compare off1 off2)
      object_slices
    |> List.map
         (fun (offset, length, dest, dest_off) ->
           Interval.({ offset; length; }), dest, dest_off)
  in

  let last_slice, length =
    (* ensuring our client doesn't ask stupid things *)
    List.fold_left
      (fun (prev, acc_length) (object_slice, _, _) ->
        let open Interval in

        if object_slice.length < 0
        then Error.(failwith BadSliceLength);

        if overlap prev object_slice
        then Error.(failwith OverlappingSlices);

        object_slice, Int64.(add acc_length (of_int object_slice.length)))
      (Interval.({ offset = -1L; length = 0; }), 0L)
      slices
  in

  assert Int64.(length <: of_int (2 lsl 31));

  (* length sanity check (this also filters out an empty slices list) *)
  if length = 0L
  then Lwt.return ([], [])
  else if Int64.(manifest.Manifest.size <:
                   add last_slice.Interval.offset (of_int last_slice.Interval.length))
  then Error.(failwith SliceOutsideObject)
  else
    begin
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
      let encryption = Encrypt_info_helper.get_encryption preset enc in

      let _, _, intersections =
        List.fold_left
          (fun (chunk_offset, chunk_id, acc) chunk_size ->
            let fragment_size = chunk_size / k in

            let rec inner acc total_offset = function
              | fragment_id when fragment_id = k -> acc
              | fragment_id ->
                 let fragment_slice = Interval.({ offset = total_offset;
                                                  length = fragment_size; }) in
                 let intersecting_slices =
                   List.fold_left
                     (fun acc (slice, dest, dest_off) ->
                       let open Interval in
                       (* TODO all slices are compared with all fragment intervals?
                        * that's not really awesome...
                        * could do sth similar to merging 2 sorted lists
                        *)
                       match intersection fragment_slice slice with
                       | None -> acc
                       | Some fragment_slice_intersection ->
                          let fragment_offset =
                            let open Int64 in
                            sub
                              fragment_slice_intersection.offset
                              total_offset
                            |> to_int
                          in
                          let dest_off =
                            dest_off +
                              (let open Int64 in
                               sub
                                 fragment_slice_intersection.offset
                                 slice.offset
                               |> to_int)
                          in
                          (fragment_offset, fragment_slice_intersection.length,
                           dest, dest_off) :: acc)
                     []
                     slices
                 in

                 let acc' =
                   if intersecting_slices = []
                   then acc
                   else (fragment_id, fragment_slice, intersecting_slices) :: acc
                 in

                 inner
                   acc'
                   Int64.(add total_offset (of_int fragment_size))
                   (fragment_id + 1)
            in
            let chunk_intersections = inner [] chunk_offset 0 in

            let acc' =
              if chunk_intersections = []
              then acc
              else (chunk_id, chunk_intersections) :: acc
            in

            (Int64.(add chunk_offset (of_int chunk_size)), chunk_id + 1, acc'))
          (0L, 0, [])
          manifest.Manifest.chunk_sizes
      in

      Lwt_list.map_s
        (fun (chunk_id, chunk_intersections) ->
          let chunk_locations = List.nth_exn fragment_info chunk_id in

          let fetch_chunk () =
            Alba_client_download.download_chunk
              ~namespace_id
              ~encryption
              ~object_id
              ~object_name
              chunk_locations ~chunk_id
              decompress
              k m w'
              osd_access
              fragment_cache
              ~cache_on_read
              bad_fragment_callback
          in

          Lwt.catch
            (fun () ->
              Lwt_unix.with_timeout
                (osd_access # osd_timeout)
                (fun () ->
                  try_get_from_fragments
                    osd_access
                    ~chunk_id
                    ~k
                    chunk_locations
                    chunk_intersections
                    ~namespace_id
                    ~object_name ~object_id
                    ~cache_on_read
                    fragment_cache
                    compression decompress
                    encryption
                    fragment_statistics_cb
                    ~partial_osd_read
                    bad_fragment_callback >>= fun mfs ->
                  Lwt.return (`Success mfs))
            )
            (fun exn ->
              let fragment_id_o = match exn with
                | Unreachable_fragment { fragment_id; } -> Some fragment_id
                | _ -> None
              in
              Lwt.return (`Failed (fetch_chunk, chunk_id, fragment_id_o, chunk_intersections))))
        intersections >>= fun r ->
      List.fold_left
        (fun (failures, mfs) ->
          function
          | `Success mfs' -> failures, List.rev_append mfs' mfs
          | `Failed x -> x :: failures, mfs)
        ([], [])
        r
      |> Lwt.return
    end

let _repair_after_read
      osd_access
      (nsm_host_access : Nsm_host_access.nsm_host_access)
      ~get_namespace_osds_info_cache
      ~get_ns_preset_info
      ~namespace_id
      manifest
      to_repair
  =
  Lwt.finalize
    (fun () ->
      get_namespace_osds_info_cache ~namespace_id >>= fun osds_info_cache ->
      get_ns_preset_info ~namespace_id >>= fun preset ->
      nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->

      let fragment_checksum_algo =
        preset.Albamgr_protocol.Protocol.Preset.fragment_checksum_algo in

      let open Nsm_model in
      let es, compression = match manifest.Manifest.storage_scheme with
        | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
      let enc = manifest.Manifest.encrypt_info in
      let encryption = Encrypt_info_helper.get_encryption preset enc in
      let version_id = manifest.Manifest.version_id + 1 in
      let Encoding_scheme.RSVM (k, _, _) = es in

      let fragment_info =
        Layout.combine
          manifest.Manifest.fragment_locations
          manifest.Manifest.fragment_checksums
      in

      Lwt_list.map_s
        (fun (chunk_id, fragment_id_o, data_fragments, coding_fragments, cleanup) ->
          Lwt_log.debug_f "repair on read fragment_id=%s" ([%show : int option] fragment_id_o) >>= fun () ->
          let with_chunk_data f = f data_fragments coding_fragments in
          Maintenance_helper.upload_missing_fragments
            osd_access
            osds_info_cache
            ~namespace_id
            manifest
            ~chunk_id
            ~version_id
            ~gc_epoch
            compression
            encryption
            fragment_checksum_algo
            ~k
            ~problem_fragments:(match fragment_id_o with
                                | Some id -> [ (chunk_id, id); ]
                                | None -> [])
            ~problem_osds:Int64Set.empty
            ~n_chunks:(List.length manifest.Manifest.fragment_locations)
            ~chunk_location:(List.nth_exn fragment_info chunk_id)
            ~with_chunk_data >>= fun updated_locations ->
          Lwt_log.debug_f "updated_locations=%s" ([%show : (int * int64) list] updated_locations) >>= fun () ->
          Lwt.return (chunk_id, updated_locations))
        to_repair
      >>= fun updated_locations ->

      nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->
      nsm # update_manifest
          ~object_name:manifest.Manifest.name
          ~object_id:manifest.Manifest.object_id
          ~gc_epoch
          ~version_id
          (List.flatten
             (List.map
                (fun (chunk_id, updated_locations) ->
                  List.map
                    (fun (fragment_id, osd_id) -> chunk_id, fragment_id, Some osd_id)
                    updated_locations)
                updated_locations))
    )
    (fun () ->
      List.iter
        (fun (_, _, _, _, cleanup) -> cleanup ())
        to_repair;
      Lwt.return ())


let handle_failures
      manifest mfs
      mgr_access
      (nsm_host_access : Nsm_host_access.nsm_host_access)
      osd_access
      ~namespace_id
      failures
      ~do_repair
      ~get_ns_preset_info
      ~get_namespace_osds_info_cache
  =
  if failures = []
  then Lwt.return mfs
  else
    begin
      Lwt_list.map_s
        (fun (fetch_chunk, chunk_id, fragment_id_o, chunk_intersections) ->

          fetch_chunk () >>= fun (data_fragments, coding_fragments, _) ->
          let () =
            List.iter
              (fun (fragment_id, fragment_slice, fragment_intersections) ->
                blit_from_fragment
                  (List.nth_exn data_fragments fragment_id)
                  fragment_intersections)
              chunk_intersections
          in
          let cleanup () =
            List.iter
              Lwt_bytes.unsafe_destroy
              coding_fragments;
            List.iter
              Lwt_bytes.unsafe_destroy
              data_fragments
          in
          let res = (chunk_id, fragment_id_o, data_fragments, coding_fragments, cleanup) in
          Lwt.return res
        )
        failures
      >>= fun to_repair ->
      let () =
        if do_repair
        then
          Lwt.async
            (fun () ->
              Lwt.catch
                (fun () ->
                  _repair_after_read
                    osd_access
                    nsm_host_access
                    ~get_namespace_osds_info_cache
                    ~get_ns_preset_info
                    ~namespace_id
                    manifest
                    to_repair)
                (fun exn ->
                  Lwt_log.debug_f ~exn "Exception in _repair_after_read, falling back to rewrite" >>= fun () ->
                  mgr_access # add_work_items
                             [ Albamgr_protocol.Protocol.Work.RewriteObject (namespace_id,
                                                                             manifest.Nsm_model.Manifest.object_id); ]
                )
            )
        else
          List.iter
            (fun (_, _, _, _, cleanup) -> cleanup ())
            to_repair
      in
      Lwt.return mfs
    end

let download_object_slices_from_fresh_manifest
      mgr_access
      nsm_host_access
      get_preset_info
      ~namespace_id
      ~manifest
      ~(object_slices : (Int64.t * int * Lwt_bytes.t * int) list)
      ~fragment_statistics_cb
      osd_access
      fragment_cache
      ~cache_on_read
      bad_fragment_callback
      ~partial_osd_read
      ~do_repair
      ~get_ns_preset_info
      ~get_namespace_osds_info_cache
  =
  _download_object_slices
      nsm_host_access
      get_preset_info
      ~namespace_id
      ~manifest
      ~object_slices
      ~fragment_statistics_cb
      osd_access
      fragment_cache
      ~cache_on_read
      bad_fragment_callback
      ~partial_osd_read >>= fun (failures, mfs) ->
  handle_failures
    manifest mfs
    mgr_access
    nsm_host_access
    osd_access
    ~namespace_id
    failures
    ~do_repair
    ~get_ns_preset_info
    ~get_namespace_osds_info_cache


let download_object_slices
      mgr_access
      nsm_host_access
      get_preset_info
      manifest_cache
      ~consistent_read
      ~namespace_id
      ~object_name
      ~(object_slices : (Int64.t * int * Lwt_bytes.t * int) list)
      ~fragment_statistics_cb
      osd_access
      fragment_cache
      ~cache_on_read
      bad_fragment_callback
      ~partial_osd_read
      ~get_ns_preset_info
      ~get_namespace_osds_info_cache
      ~do_repair
  =
  Alba_client_download.get_object_manifest'
    nsm_host_access
    manifest_cache
    ~namespace_id ~object_name
    ~consistent_read ~should_cache:true
  >>= fun (mf_source, manifest_o) ->
  match manifest_o with
  | None -> Lwt.return_none
  | Some manifest ->
     _download_object_slices
       nsm_host_access
       get_preset_info
       ~namespace_id
       ~manifest
       ~object_slices
       ~fragment_statistics_cb
       osd_access
       fragment_cache
       ~cache_on_read
       bad_fragment_callback
       ~partial_osd_read >>= fun (failures, mfs) ->
     if failures = []
     then Lwt.return (Some (manifest, namespace_id, mf_source, mfs))
     else
       begin
         match mf_source with
         | Cache.Stale
         | Cache.Slow ->
            handle_failures
              manifest mfs
              mgr_access
              nsm_host_access
              osd_access
              ~namespace_id
              failures
              ~get_ns_preset_info
              ~get_namespace_osds_info_cache
              ~do_repair >>= fun mfs ->
            Lwt.return (Some (manifest, namespace_id, mf_source, mfs))
         | Cache.Fast ->
            Alba_client_download.get_object_manifest'
              nsm_host_access
              manifest_cache
              ~namespace_id
              ~object_name
              ~consistent_read:true
              ~should_cache:true >>= fun (mf_source, manifest_o) ->
            match manifest_o with
            | None -> Lwt.return_none
            | Some manifest' ->
               if manifest <> manifest'
               then
                 begin
                   download_object_slices_from_fresh_manifest
                     mgr_access
                     nsm_host_access
                     get_preset_info
                     ~namespace_id
                     ~manifest:manifest'
                     ~object_slices
                     ~fragment_statistics_cb
                     osd_access
                     fragment_cache
                     ~cache_on_read
                     bad_fragment_callback
                     ~partial_osd_read
                     ~get_ns_preset_info
                     ~get_namespace_osds_info_cache
                     ~do_repair >>= fun mfs ->
                   Lwt.return (Some (manifest', namespace_id, Cache.Stale, mfs))
                 end
               else
                 begin
                   handle_failures
                     manifest mfs
                     mgr_access
                     nsm_host_access
                     osd_access
                     ~namespace_id
                     failures
                     ~get_ns_preset_info
                     ~get_namespace_osds_info_cache
                     ~do_repair >>= fun mfs ->
                   Lwt.return (Some (manifest', namespace_id, Cache.Fast, mfs))
                 end
       end
