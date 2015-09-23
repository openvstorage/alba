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

open Lwt_bytes2
open Slice
open Alba_statistics
open Osd_access
open Lwt.Infix

let get_object_manifest'
      nsm_host_access
      manifest_cache
      ~namespace_id ~object_name
      ~consistent_read ~should_cache =
  Lwt_log.debug_f
    "get_object_manifest %li %S ~consistent_read:%b ~should_cache:%b"
    namespace_id object_name consistent_read should_cache
  >>= fun () ->
  let lookup_on_nsm_host namespace_id object_name =
    nsm_host_access # get_nsm_by_id ~namespace_id >>= fun client ->
    client # get_object_manifest_by_name object_name
  in
  Manifest_cache.ManifestCache.lookup
    manifest_cache
    namespace_id object_name
    lookup_on_nsm_host
    ~consistent_read ~should_cache


(* consumers of this method are responsible for freeing
 * the returned fragment bigstring
 *)
let download_fragment
      (osd_access : osd_access)
      (* TODO Nsm_model.location instead? *)
      ~osd_id_o ~version_id
      ~namespace_id
      ~object_id ~object_name
      ~chunk_id ~fragment_id
      ~replication
      ~fragment_checksum
      decompress
      ~encryption
      fragment_cache
      bad_fragment_callback
  =
  (match osd_id_o with
   | None -> Lwt.fail_with "can't download fragment from None osd"
   | Some osd_id -> Lwt.return osd_id)
  >>= fun osd_id ->

  let t0_fragment = Unix.gettimeofday () in

  let key_string =
    Osd_keys.AlbaInstance.fragment
      ~namespace_id
      ~object_id ~version_id
      ~chunk_id ~fragment_id
  in
  let key = Slice.wrap_string key_string in

  let retrieve key =
    fragment_cache # lookup
                   namespace_id key_string
    >>= function
    | None ->
       begin
         Lwt_log.debug_f "fragment not in cache, trying osd:%li" osd_id
         >>= fun () ->

         Lwt.catch
           (fun () ->
            osd_access # with_osd
                       ~osd_id
                       (fun device_client ->
                        device_client # get_option key))
           (let open Asd_protocol.Protocol in
            function
            | (Error.Exn err) as exn ->
               begin match err with
                     | Error.Unknown_error _
                     | Error.ProtocolVersionMismatch _ ->
                        bad_fragment_callback
                          ~namespace_id ~object_id ~object_name
                          ~chunk_id ~fragment_id ~version_id
                     | Error.Full (* a bit silly as this is not an update *)
                     | Error.Assert_failed _
                     | Error.Unknown_operation ->
                        ()
               end;
               Lwt.fail exn
            | exn -> Lwt.fail exn)
         >>= function
         | None ->
            let msg =
              Printf.sprintf
                "Detected missing fragment namespace_id=%li object_id=%S osd_id=%li (chunk,fragment,version)=(%i,%i,%i)"
                namespace_id object_id osd_id
                chunk_id fragment_id version_id
            in
            Lwt_log.warning msg >>= fun () ->
            bad_fragment_callback
              ~namespace_id ~object_id ~object_name
              ~chunk_id ~fragment_id ~version_id;
            (* TODO loopke die queue harvest en nr albamgr duwt *)
            (* TODO testje *)
            Lwt.fail_with msg
         | Some (data:Slice.t) ->
            osd_access # get_osd_info ~osd_id >>= fun (_, state) ->
            state.read <- Unix.gettimeofday () :: state.read;
            Lwt.ignore_result
              (fragment_cache # add
                              namespace_id key_string
                              (Slice.get_string_unsafe data)
              );
            let hit_or_mis = false in
            Lwt.return (hit_or_mis, data)
       end
    | Some data ->
       let hit_or_mis = true in
       Lwt.return (hit_or_mis, Slice.wrap_string data)
  in
  Statistics.with_timing_lwt (fun () -> retrieve key)

  >>= fun (t_retrieve, (hit_or_miss, fragment_data)) ->

  let fragment_data' = Slice.to_bigstring fragment_data in

  Statistics.with_timing_lwt
    (fun () ->
     Fragment_helper.verify fragment_data' fragment_checksum)
  >>= fun (t_verify, checksum_valid) ->

  (if checksum_valid
   then Lwt.return ()
   else
     begin
       Lwt_bytes.unsafe_destroy fragment_data';
       bad_fragment_callback
         ~namespace_id ~object_id ~object_name
         ~chunk_id ~fragment_id ~version_id;
       Lwt.fail_with "Checksum mismatch"
     end) >>= fun () ->

  Statistics.with_timing_lwt
    (fun () ->
     Fragment_helper.maybe_decrypt
       encryption
       ~object_id ~chunk_id ~fragment_id
       ~ignore_fragment_id:replication
       fragment_data')
  >>= fun (t_decrypt, maybe_decrypted) ->

  Statistics.with_timing_lwt
    (fun () -> decompress ~release_input:true maybe_decrypted)
  >>= fun (t_decompress, (maybe_decompressed : Lwt_bytes.t)) ->

  let t_fragment = Statistics.({
                                  osd_id;
                                  retrieve = t_retrieve;
                                  hit_or_miss;
                                  verify = t_verify;
                                  decrypt = t_decrypt;
                                  decompress = t_decompress;
                                  total = Unix.gettimeofday () -. t0_fragment;
                                }) in

  Lwt.return (t_fragment, maybe_decompressed)
