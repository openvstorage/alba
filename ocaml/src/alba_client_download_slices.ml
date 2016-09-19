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
      ~replication
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
          ~replication
          ~fragment_checksum
          decompress
          ~encryption
          ~cache_on_read
          fragment_cache
          bad_fragment_callback)
      (fun exn ->
        Lwt_log.debug_f
          ~exn
          "Exception while downloading fragment namespace_id=%li object_name,id=(%S,%S) chunk,fragment=(%i,%i) location=%s"
          namespace_id
          object_name object_id
          chunk_id fragment_id
          ([%show : int32 option * int] location) >>= fun () ->
        Lwt.fail (Unreachable_fragment { chunk_id; fragment_id; }))
    >>= fun (t_fragment, fragment_data) ->
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
             | Success -> Lwt.return_true
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
        | true ->
           (* read fragment pieces from the fragment cache *)
           Lwt.return_unit
        | false ->
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
  get_from_fragments ()


let _download_object_slices
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
  then Lwt.return []
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
                    ~replication:(k=1)
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
                    bad_fragment_callback >>= fun () ->
                  Lwt.return `Success)
            )
            (fun exn ->
              let fragment_id_o = match exn with
                | Unreachable_fragment { fragment_id; } -> Some fragment_id
                | _ -> None
              in
              Lwt.return (`Failed (fetch_chunk, chunk_id, fragment_id_o, chunk_intersections))))
        intersections
    end

let handle_failures failures ~do_repair =
  if failures = []
  then Lwt.return ()
  else
    begin
      (* TODO maybe do repair
       * - fragment that had issues (if any)
       * - all disqualified osds?
       * - data fragments on None osd
       * - maybe fill in None for some coding fragments if needed (so the osds become available for data fragments)
       *
       * if not possible (policy not satisfiable): rewrite object
       *)

      Lwt_list.iter_p
        (fun (fetch_chunk, chunk_id, fragment_id_o, chunk_intersections) ->

          let fetch_chunk () =
            fetch_chunk ()
            >>= fun (data_fragments, coding_fragments, t_chunk) ->
            List.iter
              Lwt_bytes.unsafe_destroy
              coding_fragments;
            Lwt.return data_fragments
          in

          let get_from_chunk () =
            fetch_chunk () >>= fun data_fragments ->
            let () =
              finalize
                (fun () ->
                  List.iter
                    (fun (fragment_id, fragment_slice, fragment_intersections) ->
                      blit_from_fragment
                        (List.nth_exn data_fragments fragment_id)
                        fragment_intersections)
                    chunk_intersections)
                (fun () ->
                  List.iter
                    Lwt_bytes.unsafe_destroy
                    data_fragments)
            in
            Lwt.return_unit
          in

          get_from_chunk ())
        failures
    end

let download_object_slices_from_fresh_manifest
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
      ~partial_osd_read >>= fun results ->
  let failures =
    List.fold_left
      (fun acc ->
        function
        | `Success -> acc
        | `Failed x -> x :: acc)
      []
      results
  in
  handle_failures failures ~do_repair


let download_object_slices
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
       ~partial_osd_read >>= fun results ->
     let failures =
       List.fold_left
         (fun acc ->
           function
           | `Success -> acc
           | `Failed x -> x :: acc)
         []
         results
     in
     if failures = []
     then Lwt.return (Some (manifest, namespace_id, mf_source))
     else
       begin
         match mf_source with
         | Cache.Stale
         | Cache.Slow ->
            handle_failures failures ~do_repair:false >>= fun () ->
            Lwt.return (Some (manifest, namespace_id, mf_source))
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
                     ~do_repair:false >>= fun () ->
                   Lwt.return (Some (manifest', namespace_id, Cache.Stale))
                 end
               else
                 begin
                   handle_failures failures ~do_repair:false >>= fun () ->
                   Lwt.return (Some (manifest', namespace_id, Cache.Fast))
                 end
       end
