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
open Checksum
open Lwt.Infix

type object_name = string
type update =
  | UploadObjectFromReader of object_name * Object_reader.reader * Checksum.t option
  | DeleteObject of object_name

let apply_sequence
      (alba_client : Alba_client.alba_client)
      (* namespace *) namespace_id
      (assert_ts : Nsm_model.Assert.t list Lwt.t)
      (updates : update list)
      (* stats *)
  =
  let t0 = Unix.gettimeofday () in
  let upload object_name object_reader checksum_o =
    Alba_client_upload.upload_object''
      (alba_client # nsm_host_access)
      (alba_client # osd_access)
      (alba_client # get_base_client # get_preset_cache # get)
      (alba_client # get_base_client # get_namespace_osds_info_cache)
      ~object_t0:t0
      ~timestamp:t0
      ~namespace_id
      ~object_name
      ~object_reader
      ~checksum_o
      ~object_id_hint:None
      ~fragment_cache:(alba_client # get_base_client # get_fragment_cache)
      ~cache_on_write:(alba_client # get_base_client # get_cache_on_read_write |> snd)
    >>= fun (mf, extra_mfs, upload_stats, gc_epoch) ->

    let all_mfs = (mf.Nsm_model.Manifest.name, "", (mf, namespace_id)) ::
                    (List.map
                       (fun (mf, namespace_id, alba) ->
                         mf.Nsm_model.Manifest.name, alba, (mf, namespace_id))
                       extra_mfs)
    in
    Lwt.return (Nsm_model.Update.PutObject (mf, gc_epoch), all_mfs, `Upload (mf, gc_epoch, upload_stats))
  in

  Lwt_list.map_p
    (function
     | UploadObjectFromReader (object_name, object_reader, cs_o) ->
        upload object_name object_reader cs_o
     | DeleteObject object_name ->
        Lwt.return (Nsm_model.Update.DeleteObject object_name, [], `Delete object_name)
    )
    updates >>= fun updates ->
  let updates, manifests, upload_statss = List.split3 updates in
  let manifests = List.flatten_unordered manifests in

  assert_ts >>= fun asserts ->

  let manifest_cache = alba_client # get_manifest_cache in

  alba_client # nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->
  Lwt.catch
    (fun () ->
      nsm # apply_sequence asserts updates)
    (fun exn ->
      (* clear the manifest cache in error path *)
      List.iter
        (function
         | `Upload (manifest, _, _) ->
            Manifest_cache.ManifestCache.remove manifest_cache namespace_id manifest.Nsm_model.Manifest.name
         | `Delete object_name ->
            Manifest_cache.ManifestCache.remove manifest_cache namespace_id object_name)
        upload_statss;

      Lwt.fail exn)
  >>= fun () ->

  (* update manifest cache *)
  let t1 = Unix.gettimeofday () in
  let delta = t1 -. t0 in
  List.iter
    (function
     | `Upload (manifest, gc_epoch, upload_stats) ->
        Alba_client_upload.store_manifest_epilogue
          (alba_client # osd_access)
          (alba_client # get_manifest_cache)
          manifest
          gc_epoch
          ~namespace_id
          (upload_stats delta)
     | `Delete object_name ->
        Manifest_cache.ManifestCache.remove
          (alba_client # get_manifest_cache) namespace_id object_name
    )
    upload_statss;

  Lwt.return (manifests, delta)
