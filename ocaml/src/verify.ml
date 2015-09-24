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
open Lwt_bytes2
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

  let osd_access = alba_client # osd_access in

  Lwt_list.mapi_s
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
                 if verify_checksum
                 then
                   osd # multi_get [ key ] >>= function
                   | [ None ] -> Lwt.return `Missing
                   | [ Some f ] ->
                      let checksum = Layout.index manifest.fragment_checksums chunk_id fragment_id in
                      let fragment_data' = Slice.to_bigstring f in
                      Fragment_helper.verify fragment_data' checksum
                      >>= fun checksum_valid ->
                      Lwt_bytes.unsafe_destroy fragment_data';
                      Lwt.return
                        (if checksum_valid
                         then `Ok
                         else `ChecksumMismatch)
                   | _ -> assert false
                 else
                   osd # multi_exists [ key ] >>= function
                   | [ true; ] -> Lwt.return `Ok
                   | [ false; ] -> Lwt.return `Missing
                   | _ -> assert false))
             (fun exn ->
              Lwt.return `Unavailable))
       chunk_location)
    manifest.Manifest.fragment_locations >>= fun results ->

  let _, problem_fragments =
    List.fold_left
      (fun (chunk_id, acc) ls ->
       let _, acc' =
         List.fold_left
           (fun (fragment_id, acc) status ->
            let should_repair =
              match status with
              | `NoneOsd
              | `Ok -> false
              | `ChecksumMismatch
              | `Missing -> true
              | `Unavailable -> repair_osd_unavailable
            in
            (fragment_id + 1,
             if should_repair
             then (chunk_id, fragment_id) :: acc
             else acc))
           (0, acc)
           ls
       in
       (chunk_id + 1, acc'))
      (0, [])
      results
  in

  (if problem_fragments <> []
   then
     begin
       Lwt_log.debug_f
         "verify results in repairing fragments: %s"
         ([%show : (int * int) list] problem_fragments) >>= fun () ->

       Lwt.catch
         (fun () ->
          (* TODO use maintenance_client # repair_object instead? *)
          Repair.repair_object_generic_and_update_manifest
            alba_client
            ~namespace_id
            ~manifest
            ~problem_osds:Int32Set.empty
            ~problem_fragments)
         (fun exn ->
          Repair.rewrite_object
            alba_client
            ~namespace_id
            ~manifest)
     end
   else
     Lwt.return ()) >>= fun () ->

  (* TODO report back on what happened... *)
  Lwt.return []
