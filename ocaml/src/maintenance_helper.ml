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
open Nsm_model
open Recovery_info
open Lwt.Infix

let upload_missing_fragments
      (alba_client : Alba_base_client.client)
      osds_info_cache'
      ok_fragments
      all_fragments
      fragments_to_be_repaired
      ~namespace_id
      manifest
      ~chunk_id
      ~version_id
      ~gc_epoch
      locations
      compression
      encryption
      fragment_checksum_algo
      ~is_replication
  =
  let ok_fragments' =
    List.map_filter_rev
      Std.id
      ok_fragments
  in

  (* update osds_info_cache' to make sure it contains all
                osds that will be force chosen *)
  Lwt_list.iter_p
    (fun osd_id ->
     alba_client # osd_access # get_osd_info ~osd_id >>= fun (osd_info, _) ->
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
    let open Nsm_model in
    Hashtbl.fold
      (fun id (info:OsdInfo.t) acc ->
       if info.OsdInfo.decommissioned
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

  let object_id = manifest.Manifest.object_id in

  alba_client # nsm_host_access # get_namespace_info ~namespace_id
  >>= fun (namespace, _, _, _) ->

  RecoveryInfo.make
    ~namespace
    ~object_name:manifest.Manifest.name
    ~object_id
    object_info_o
    encryption
    (List.nth_exn manifest.Manifest.chunk_sizes chunk_id)
    (List.nth_exn manifest.Manifest.fragment_packed_sizes chunk_id)
    (List.nth_exn manifest.Manifest.fragment_checksums chunk_id)
  >>= fun recovery_info_slice ->

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
     >>= fun (packed_fragment, _, _, checksum') ->

     if checksum = checksum'
     then
       begin
         Alba_client_upload.upload_packed_fragment_data
           (alba_client # osd_access)
           ~namespace_id
           ~osd_id:chosen_osd_id
           ~object_id ~version_id
           ~chunk_id ~fragment_id
           ~packed_fragment
           ~gc_epoch ~checksum
           ~recovery_info_blob:(Osd.Blob.Slice recovery_info_slice)
         >>= fun () ->
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
    to_be_repaireds
