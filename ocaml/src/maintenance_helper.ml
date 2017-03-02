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
open Nsm_model
open Recovery_info
open Lwt.Infix

let choose_new_devices
      (osd_access : Osd_access_type.t)
      osds_info_cache'
      osds_to_keep
      no_fragments_to_be_repaired
  =
  (* update osds_info_cache' to make sure it contains all
   * osds that will be force chosen *)
  Lwt_list.iter_p
    (fun osd_id ->
     osd_access # get_osd_info ~osd_id >>= fun (osd_info, _, _) ->
     Hashtbl.replace osds_info_cache' osd_id osd_info;
     Lwt.return ())
    osds_to_keep >>= fun () ->

  let extra_devices =
    Choose.choose_extra_devices
      no_fragments_to_be_repaired
      osds_info_cache'
      osds_to_keep
  in
  Lwt.return extra_devices

let _upload_missing_fragments
      osd_access
      osds_info_cache'
      ok_fragments
      all_fragments
      fragments_to_be_repaired
      ~namespace_id
      manifest
      ~chunk_id
      ~version_id
      ~gc_epoch
      compression
      encryption
      fragment_checksum_algo
      ~is_replication
      ~n_chunks ~chunk_fragments
  =

  let ok_fragments' =
    List.map_filter_rev
      Std.id
      ok_fragments
  in

  choose_new_devices
    osd_access
    osds_info_cache'
    ok_fragments'
    (List.length fragments_to_be_repaired)
  >>= fun extra_devices ->

  let live_ones =
    let open Nsm_model in
    Hashtbl.fold
      (fun id (info:OsdInfo.t) acc ->
       if info.OsdInfo.decommissioned
       then acc else id :: acc
      )
      osds_info_cache' []
  in
  let extra_device_ids = List.map fst extra_devices in
  let show = [%show: int64 list] in
  Lwt_log.debug_f
    "extra_devices: live_ones:%s ok=%s extra:%s"
    (show live_ones)
    ([%show : int64 option list] ok_fragments)
    (show extra_device_ids)
  >>= fun () ->

  let () =
    (* sanity check *)
    List.iter
      (fun extra_id ->
       assert (not (List.mem extra_id ok_fragments'))
      )
      extra_device_ids
  in

  let to_be_repaireds =
    List.map2
      (fun fragment_id (osd_id, _) -> (fragment_id, osd_id))
      fragments_to_be_repaired
      extra_devices in

  let object_info_o =
    let is_last_chunk = chunk_id = n_chunks - 1 in
    if is_last_chunk
    then Some {
             RecoveryInfo.storage_scheme = manifest.Manifest.storage_scheme;
             size = manifest.Manifest.size;
             checksum = manifest.Manifest.checksum;
             timestamp = manifest.Manifest.timestamp;
             }
    else None
  in

  let object_id = manifest.Manifest.object_id in

  Lwt_list.map_p
    (fun ((fragment_id, checksum), chosen_osd_id) ->
      let fragment_ba = List.nth_exn all_fragments fragment_id in
      Fragment_helper.pack_fragment
        (Bigstring_slice.wrap_bigstring fragment_ba)
        ~object_id ~chunk_id ~fragment_id
        ~ignore_fragment_id:is_replication
        compression
        encryption
        fragment_checksum_algo
      >>= fun (packed_fragment, _, _, checksum', fragment_ctr) ->

      let old_f_checksums_for_chunk =
        List.nth_exn manifest.Manifest.fragments chunk_id
        |> List.map Fragment.crc_of
      in
      let old_fp_sizes_for_chunk =
        List.nth_exn manifest.Manifest.fragments chunk_id
        |> List.map Fragment.len_of
      in
      RecoveryInfo.make
        ~object_name:manifest.Manifest.name
        ~object_id
        object_info_o
        encryption
        (List.nth_exn manifest.Manifest.chunk_sizes chunk_id)
        old_fp_sizes_for_chunk
        old_f_checksums_for_chunk
        fragment_ctr
      >>= fun recovery_info_slice ->
      let recovery_info_blob_old = Osd.Blob.Slice recovery_info_slice in

      (if checksum = checksum'
       then Lwt.return (None, checksum, recovery_info_blob_old)
       else
       begin
         Lwt_log.debug_f
           "checksum changed for packed_fragment (%i,%i) %s -> %s (compression:%s)"
           chunk_id fragment_id
           (Checksum.show checksum) (Checksum.show checksum')
           (Compression.show compression)
         >>= fun () ->
         let packed_size' = Lwt_bytes2.Lwt_bytes.length packed_fragment in
         let new_f_checksums_for_chunk =
           List.replace fragment_id checksum' old_f_checksums_for_chunk
         in

         let new_fp_sizes_for_chunk =
           List.replace fragment_id packed_size' old_fp_sizes_for_chunk
         in
         RecoveryInfo.make
           ~object_name:manifest.Manifest.name
           ~object_id
           object_info_o
           encryption
           (List.nth_exn manifest.Manifest.chunk_sizes chunk_id)
           new_fp_sizes_for_chunk
           new_f_checksums_for_chunk
           fragment_ctr
         >>= fun recovery_info' ->
         Lwt.return
           (Some (packed_size', checksum'),
            checksum',
            (Osd.Blob.Slice recovery_info'))
       end)
     >>= fun (maybe_changed, new_checksum, recovery_info_blob) ->
     begin
       Alba_client_upload.upload_packed_fragment_data
         osd_access
         ~namespace_id
         ~osd_id:chosen_osd_id
         ~object_id ~version_id
         ~chunk_id ~fragment_id
         ~packed_fragment
         ~gc_epoch ~checksum:new_checksum
         ~recovery_info_blob
       >>= fun fnro ->
       Lwt.return (fragment_id,
                   Some chosen_osd_id,
                   maybe_changed,
                   fragment_ctr, fnro)
     end
    )
    to_be_repaireds

let upload_missing_fragments
      osd_access
      osds_info_cache'
      ~namespace_id
      manifest
      ~chunk_id
      ~version_id
      ~gc_epoch
      compression
      encryption
      fragment_checksum_algo
      ~k
      ~problem_fragments ~problem_osds
      ~n_chunks ~chunk_fragments
      ~with_chunk_data
  =

  let problem_osds =
    (* always repair fragments of all disqualified osds too *)
    Hashtbl.fold
      (fun osd_id (_, state, _) acc ->
        if Osd_state.disqualified state
        then Int64Set.add osd_id acc
        else acc)
      (osd_access # osds_info_cache)
      problem_osds
  in

  let _, ok_fragments, fragments_to_be_repaired =
    List.fold_left
      (fun (fragment_id, ok_fragments, to_be_repaireds)
           fragment ->
        let (fragment_osd_id_o, fragment_version_id),
            fragment_checksum,
            fragment_ctr = fragment
        in
        let ok_fragments', to_be_repaireds' =
          if List.mem (chunk_id, fragment_id) problem_fragments ||
               (match fragment_osd_id_o with
                | None -> (fragment_id < k) (* repair missing data fragments *)
                | Some osd_id -> Int64Set.mem osd_id problem_osds)
          then
            ok_fragments,
            (fragment_id, fragment_checksum) :: to_be_repaireds
          else
            fragment_osd_id_o :: ok_fragments,
            to_be_repaireds in
        fragment_id + 1, ok_fragments', to_be_repaireds')
      (0, [], [])
      chunk_fragments in

  if fragments_to_be_repaired = []
  then Lwt.return []
  else
    with_chunk_data
      (fun data_fragments coding_fragments ->
        let all_fragments = List.append data_fragments coding_fragments in
        _upload_missing_fragments
          osd_access
          osds_info_cache'
          ok_fragments
          all_fragments
          fragments_to_be_repaired
          ~namespace_id
          manifest
          ~chunk_id
          ~version_id
          ~gc_epoch
          compression
          encryption
          fragment_checksum_algo
          ~is_replication:(k=1)
          ~n_chunks ~chunk_fragments)
