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
open Lwt_bytes2
open Lwt.Infix

let repair_object_generic
      (alba_client : Alba_base_client.client)
      ~namespace_id
      ~manifest
      ~problem_osds
      ~problem_fragments
  =
  let open Nsm_model in
  Lwt_log.debug_f "_repair_object_generic ~namespace_id:%Li ~manifest:%s ~problem_osds:%s ~problem_fragments:%s"
                  namespace_id (Manifest.show manifest)
                  ([%show : int64 list] (Int64Set.elements problem_osds))
                  ([%show : (int * int) list] problem_fragments)
  >>= fun () ->

  let object_name = manifest.Manifest.name in
  let object_id = manifest.Manifest.object_id in

  let locations = Manifest.locations manifest in
  let fragment_info = Manifest.combined_fragment_infos manifest in

  alba_client # get_ns_preset_info ~namespace_id >>= fun preset ->
  alba_client # nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->

  let fragment_checksum_algo = preset.Preset.fragment_checksum_algo in

  let es, compression = match manifest.Manifest.storage_scheme with
    | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
  let enc = manifest.Manifest.encrypt_info in
  let encryption = Encrypt_info_helper.get_encryption preset enc in
  let decompress = Fragment_helper.maybe_decompress compression in
  let k, m, w = match es with
    | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
  let w' = Encoding_scheme.w_as_int w in

  let version_id = manifest.Manifest.version_id + 1 in

  Lwt_list.map_s
    (fun (chunk_id, chunk_fragments) ->

      alba_client # get_namespace_osds_info_cache ~namespace_id >>= fun osds_info_cache' ->

      let with_chunk_data f =
        alba_client # download_chunk
                    ~namespace_id
                    ~object_id
                    ~object_name
                    chunk_fragments
                    ~chunk_id
                    ~encryption
                    decompress
                    k m w'
                    ~download_strategy:Alba_client_download.AllFragments
        >>= fun (data_fragments, coding_fragments, t_chunk) ->
        Lwt.finalize
          (fun () -> f data_fragments coding_fragments)
          (fun () ->
            List.iter
              Lwt_bytes.unsafe_destroy
              data_fragments;
            List.iter
              Lwt_bytes.unsafe_destroy
              coding_fragments;
            Lwt.return ())
      in

      Maintenance_helper.upload_missing_fragments
        (alba_client # osd_access)
        osds_info_cache'
        ~namespace_id
        manifest
        ~chunk_id ~version_id ~gc_epoch
        compression encryption
        fragment_checksum_algo
        ~k:1
        ~problem_fragments ~problem_osds
        ~n_chunks:(List.length locations)
        ~chunk_fragments
        ~with_chunk_data
      >>= fun updated_locations ->

      Lwt.return (chunk_id, updated_locations))
    (List.mapi (fun i lc -> i, lc) fragment_info)
  >>= fun updated_locations ->

  let updated_object_locations =
    List.fold_left
      (fun acc (chunk_id, updated_locations) ->
       let updated_chunk_locations =
         List.map
           (fun (fragment_id, device_id,maybe_changed, fragment_ctr,
                 apply_result') ->
             (chunk_id, fragment_id, device_id, maybe_changed,
              fragment_ctr))
           updated_locations in
       List.rev_append updated_chunk_locations acc)
      []
      updated_locations
  in

  Lwt.return (updated_object_locations, gc_epoch, version_id)

let repair_object_generic_and_update_manifest
      alba_client
      ~namespace_id
      ~manifest
      ~problem_osds
      ~problem_fragments =
  repair_object_generic
    alba_client
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
                      updated_object_locations
                      ~gc_epoch ~version_id)
  >>= fun () ->
  Lwt_log.debug_f
    "updated_manifest ~namespace_id:%Li ~object_id:%S ~updated_object_locations:%s ~version_id:%i"
    namespace_id object_id
    ([%show : (Manifest.fragment_update) list] updated_object_locations)
    version_id
  >>= fun () ->
  Lwt.return ()

let rewrite_object
      alba_client
      ~namespace_id
      ~manifest =
  let open Nsm_model in

  Lwt_log.debug_f
    "Rewriting %s in namespace %Li"
    (Manifest.show manifest) namespace_id
  >>= fun () ->

  let object_reader =
    new Object_reader2.object_reader
        alba_client
        namespace_id manifest in

  let object_name = manifest.Manifest.name in
  let checksum_o = Some manifest.Manifest.checksum in
  let old_object_id = manifest.Manifest.object_id in
  let allow_overwrite = PreviousObjectId old_object_id in
  let object_id_hint =
    Some (get_random_string 32)
    (* TODO: this causes some objects to be rewritten multiple times*)
  in

  Lwt.catch
    (fun () ->
      alba_client # upload_object'
                  ~epilogue_delay:None
                  ~namespace_id
                  ~object_name
                  ~object_reader
                  ~checksum_o
                  ~allow_overwrite
                  ~object_id_hint
                  ~timestamp:manifest.Manifest.timestamp
      >>= fun _ ->
     Lwt.return ())
    (let open Nsm_model.Err in
     function
     | Nsm_exn (Overwrite_not_allowed, _) ->
        (* ignore this one ... the object was overwritten
         * already in the meantime, which is just fine when we're
         * trying to rewrite it
         *)
        Lwt.return ()
     | exn -> Lwt.fail exn)
