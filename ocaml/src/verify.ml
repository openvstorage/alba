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

  let verify =
    if verify_checksum
    then
      fun (osd : Osd.osd) key ~chunk_id ~fragment_id ~osd_id ->
      (osd # namespace_kvs namespace_id) # get_option
          Osd.Low
          key >>= function
      | None -> Lwt.return `Missing
      | Some fragment_data ->
         let checksum =
           Manifest.get_checksum manifest chunk_id fragment_id
         in
         Fragment_helper.verify fragment_data checksum
         >>= fun checksum_valid ->
         Lwt_bytes.unsafe_destroy fragment_data;
         Lwt.return
           (if checksum_valid
            then `Ok
            else (`ChecksumMismatch osd_id))
    else
      fun osd key ~chunk_id ~fragment_id ~osd_id ->
      (* TODO verify multiple objects at once? *)
      (osd # namespace_kvs namespace_id) # multi_exists
          Osd.Low
          [ key ] >>= function
      | [ true; ] -> Lwt.return `Ok
      | [ false; ] -> Lwt.return `Missing
      | _ -> assert false
  in

  let verify ~osd_id ~chunk_id ~fragment_id ~version_id =
    let key_string =
      Osd_keys.AlbaInstance.fragment
        ~object_id ~version_id
        ~chunk_id ~fragment_id
    in
    let key = Slice.wrap_string key_string in
    Lwt.catch
      (fun () ->
       osd_access # with_osd
         ~osd_id
         (fun osd -> verify osd key ~chunk_id ~fragment_id ~osd_id))
      (fun exn ->
       Lwt.return `Unavailable)
  in

  Lwt_list.mapi_s
    (fun chunk_id chunk ->
      Lwt_list.mapi_p
        (fun fragment_id fragment ->
          let location = Manifest._loc_of fragment in
          match location with
          | (None, _) -> Lwt.return `NoneOsd
          | (Some osd_id, version_id) ->
             verify ~osd_id ~chunk_id ~fragment_id ~version_id)
        chunk)
    manifest.Manifest.fragments >>= fun results ->

  let _,
      (fragments_detected_missing,
       fragments_osd_unavailable,
       fragments_checksum_mismatch,
       problem_fragments) =

    let incr_count fcm osd_id =
      let c0 = try Int64Map.find osd_id fcm with Not_found -> 0 in
      Int64Map.add osd_id (c0 + 1) fcm
    in
    let empty = Int64Map.empty in

    List.fold_left
      (fun (chunk_id, (fragments_detected_missing,
                       fragments_osd_unavailable,
                       fragments_checksum_mismatch,
                       problem_fragments))
           ls ->

        let _, acc =
          List.fold_left
            (fun (fragment_id,
                  (fragments_detected_missing,
                   fragments_osd_unavailable,
                   fragments_checksum_mismatch,
                   problem_fragments)) status ->
              let fragments_detected_missing = match status with
                | `Missing -> fragments_detected_missing + 1
                | _        -> fragments_detected_missing
              in
              let fragments_osd_unavailable = match status with
                | `Unavailable -> fragments_osd_unavailable + 1
                | _            -> fragments_osd_unavailable
              in
              let fragments_checksum_mismatch = match status with
                | `ChecksumMismatch osd_id ->
                   incr_count fragments_checksum_mismatch osd_id
                | _                        ->
                   fragments_checksum_mismatch
              in
              let should_repair =
                match status with
                | `NoneOsd
                  | `Ok -> false
                | `ChecksumMismatch _
                  | `Missing -> true
                | `Unavailable -> repair_osd_unavailable
              in
              fragment_id + 1,
              (fragments_detected_missing,
               fragments_osd_unavailable,
               fragments_checksum_mismatch,
               if should_repair
               then (chunk_id, fragment_id) :: problem_fragments
               else problem_fragments))
            (0,
             (fragments_detected_missing,
              fragments_osd_unavailable,
              fragments_checksum_mismatch,
              problem_fragments))
            ls
        in
        chunk_id + 1,
        acc)
      (0, (0,0,empty ,[]))
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
            ~problem_osds:Int64Set.empty
            ~problem_fragments)
         (fun exn ->
          Repair.rewrite_object
            alba_client
            ~namespace_id
            ~manifest)
     end
   else
     Lwt.return ()) >>= fun () ->

  Lwt.return
    (fragments_detected_missing,
     fragments_osd_unavailable,
     fragments_checksum_mismatch)
