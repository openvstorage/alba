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

let rewrite_object
      alba_client
      ~namespace_id
      ~manifest =
  let open Nsm_model in

  Lwt_log.debug_f
    "Repairing %s in namespace %li"
    (Manifest.show manifest) namespace_id
  >>= fun () ->

  let object_reader =
    new Object_reader2.object_reader
        alba_client
        namespace_id manifest in

  let object_name = manifest.Manifest.name in
  let checksum_o = Some manifest.Manifest.checksum in
  let allow_overwrite = PreviousObjectId manifest.Manifest.object_id in

  Lwt.catch
    (fun () ->
     alba_client # upload_object'
                 ~namespace_id
                 ~object_name
                 ~object_reader
                 ~checksum_o
                 ~allow_overwrite >>= fun _ ->
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
