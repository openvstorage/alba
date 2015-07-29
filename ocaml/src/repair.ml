open Prelude
open Nsm_model
open Access
open Recovery_info
open Lwt.Infix

let repair
      (nsm_host_access : nsm_host_access)
      osd_access
      ~(namespace_id : int32)
      ~(manifest : Manifest.t)
      ~(problem_fragments : (chunk_id * fragment_id) list)
      ~(get_fragments : chunk_id -> fragment_id list -> (fragment_id * Lwt_bytes.t) list Lwt.t)

      preset
      get_namespace_osds_info_cache
      get_osd_info
      upload_packed_fragment_data
  =
  Lwt_log.debug_f "repair ~namespace_id:%li ~manifest:%s ~fragments:%s"
                  namespace_id (Manifest.show manifest)
                  ([%show : (chunk_id * fragment_id) list] problem_fragments)
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

  nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->

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

  let cm = Erasure.reed_sol_vandermonde_coding_matrix k m w' in

  let version_id = manifest.Manifest.version_id + 1 in

  Lwt_list.map_s
    (fun (chunk_id, chunk_location) ->

     get_namespace_osds_info_cache ~namespace_id >>= fun osds_info_cache' ->

     let _, ok_fragments, fragments_to_be_repaired =
       List.fold_left
         (fun (fragment_id, ok_fragments, to_be_repaireds)
              ((fragment_osd_id_o, fragment_version_id), fragment_checksum) ->
          let ok_fragments', to_be_repaireds' =
            if List.mem (chunk_id, fragment_id) problem_fragments
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
         (* alba_client # download_chunk *)
         (*             ~namespace_id *)
         (*             ~object_id *)
         (*             ~object_name *)
         (*             chunk_location *)
         (*             ~chunk_id *)
         (*             ~encryption *)
         (*             decompress *)
         (*             k m w' *)
         (*             cm *)
         (* >>= fun (data_fragments, coding_fragments, t_chunk, failed_fragments) -> *)
         (* TODO try to repair failed_fragments
                as much as possible (as achievable for the given policy)
          *)
         get_fragments 
           chunk_id
           (List.map fst fragments_to_be_repaired)
         >>= fun fragments_unpacked ->

         let ok_fragments' =
           List.map_filter_rev
             Std.id
             ok_fragments
         in

         (* update osds_info_cache' to make sure it contains all
                osds that will be force chosen *)
         Lwt_list.iter_p
           (fun osd_id ->
            get_osd_info ~osd_id >>= fun (osd_info, _) ->
            Hashtbl.replace osds_info_cache' osd_id osd_info;
            Lwt.return ())
           ok_fragments' >>= fun () ->

         let extra_devices =
           Choose.choose_extra_devices
             (List.length fragments_to_be_repaired)
             osds_info_cache'
             ok_fragments'
         in
         let live_ones =
           let open Albamgr_protocol.Protocol in
           Hashtbl.fold
             (fun id (info:Osd.t) acc ->
              if info.Osd.decommissioned
              then acc else id :: acc
             )
             osds_info_cache' []
         in
         let extra_device_ids = List.map fst extra_devices in
         let show = [%show: int32 list] in
         Lwt_log.debug_f
           "extra_devices: live_ones:%s ok=%s extra:%s"
           (show live_ones)
           ([%show : int32 option list] ok_fragments)
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
           let is_last_chunk = chunk_id = List.length locations - 1 in
           if is_last_chunk
           then Some RecoveryInfo.({
                                      storage_scheme = manifest.Manifest.storage_scheme;
                                      size = manifest.Manifest.size;
                                      checksum = manifest.Manifest.checksum;
                                      timestamp = manifest.Manifest.timestamp;
                                    })
           else None
         in

         RecoveryInfo.make
           object_name
           object_id
           object_info_o
           encryption
           (List.nth manifest.Manifest.chunk_sizes chunk_id)
           (List.nth manifest.Manifest.fragment_packed_sizes chunk_id)
           (List.nth manifest.Manifest.fragment_checksums chunk_id)
         >>= fun recovery_info_slice ->

         Lwt_list.map_p
           (fun ((fragment_id, checksum), chosen_osd_id) ->
            let fragment_ba = List.assoc fragment_id fragments_unpacked in
            Fragment_helper.pack_fragment
              fragment_ba
              ~object_id ~chunk_id ~fragment_id
              ~ignore_fragment_id:(k=1)
              compression
              encryption
              fragment_checksum_algo
            >>= fun (packed_fragment, _, _, checksum') ->

            if checksum = checksum'
            then
              begin
                upload_packed_fragment_data
                  ~namespace_id
                  ~osd_id:chosen_osd_id
                  ~object_id ~version_id
                  ~chunk_id ~fragment_id
                  ~packed_fragment
                  ~gc_epoch ~checksum
                  ~recovery_info_slice >>= fun () ->
                Lwt.return (fragment_id, chosen_osd_id)
              end
            else
              begin
                let msg =
                  Printf.sprintf
                    "Error while repairing object (this should never happen): %s <> %s"
                    (Checksum.show checksum)
                    (Checksum.show checksum')
                in
                Lwt_log.warning msg >>= fun () ->
                Lwt.fail_with msg
              end)
           to_be_repaireds >>= fun updated_locations ->

         Lwt.return (chunk_id, updated_locations)
       end)
    (List.mapi (fun i lc -> i, lc) fragment_info) >>= fun _ ->

  Lwt.return ()
