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
open Lwt.Infix

let verify_and_maybe_repair_object
      (alba_client : Alba_base_client.client)
      ~namespace_id
      ~verify_checksum
      ~repair_osd_unavailable
      manifest =
  let open Nsm_model in
  let open Manifest in
  let object_id = manifest.object_id in
  let object_name = manifest.name in

  let osd_access = alba_client # osd_access in

  if not verify_checksum
  then
    begin
      Lwt_list.mapi_p
        (fun chunk_id chunk_location ->
         Lwt_list.mapi_p
           (fun fragment_id location ->
            match location with
            | (None, _) -> Lwt.return `NoneOsd
            | (Some osd_id, version_id) ->

               let key_string =
                 Osd_keys.AlbaInstance.fragment
                   ~namespace_id
                   ~object_id ~version_id
                   ~chunk_id ~fragment_id
               in
               let key = Slice.wrap_string key_string in

               Lwt.catch
                 (fun () ->
                  osd_access # with_osd
                             ~osd_id
                             (fun osd ->
                              osd # multi_exists [ key ]) >>= function
                  | [ true; ] -> Lwt.return `Ok
                  | [ false; ] -> Lwt.return `Missing
                  | _ -> assert false)
                 (fun exn ->
                  Lwt.return `Unavailable))
           chunk_location)
        manifest.Manifest.fragment_locations >>= fun results ->

      (* TODO per chunk:
       * - needs_repair = are there missing (or unavailable) fragments
       * - repair can be done by restoring fragments or needs rewrite?
       * => be lazy ... try repair, and if it fails do a rewrite
       *    just like is done in decommission
       *
       * repairing `NoneOsd while at it would be nice but is not needed
       *)
      Lwt.return []
    end
  else
    begin
      let es, compression = match manifest.Manifest.storage_scheme with
        | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
      let enc = manifest.Manifest.encrypt_info in
      let decompress = Fragment_helper.maybe_decompress compression in
      let k, m, w = match es with
        | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
      let replication = k = 1 in

      let open Albamgr_protocol.Protocol in
      alba_client # nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
      alba_client # get_preset_info ~preset_name:ns_info.Namespace.preset_name >>= fun preset ->
      let encryption = Preset.get_encryption preset enc in

      Lwt_list.mapi_p
        (fun chunk_id chunk_location ->
         Lwt_list.mapi_p
           (fun fragment_id location ->

            let fragment_checksum =
              Layout.index manifest.fragment_checksums
                           chunk_id fragment_id
            in

            Alba_client_download.download_fragment
              (alba_client # osd_access)
              ~location
              ~namespace_id
              ~object_id ~object_name
              ~chunk_id ~fragment_id
              ~replication
              ~fragment_checksum
              decompress
              ~encryption
              (alba_client # get_fragment_cache)
            >>= fun _ ->
            (* TODO inspect result *)
            Lwt.return ())
           chunk_location)
        manifest.Manifest.fragment_locations
        (* TODO
         * - get all fragments, check result...
         *   maybe change return type van download fragment...
         * - do inline repair where needed
         *)
    end
