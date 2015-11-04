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

module E = Prelude.Error.Lwt
let (>>==) = E.bind

(* consumers of this method are responsible for freeing
 * the returned fragment bigstring
 *)
let download_packed_fragment
      (osd_access : osd_access)
      ~location
      ~namespace_id
      ~object_id ~object_name
      ~chunk_id ~fragment_id
      (fragment_cache : Fragment_cache.cache)
  =

  let osd_id_o, version_id = location in

  (match osd_id_o with
   | None -> E.fail `NoneOsd
   | Some osd_id -> E.return osd_id)
  >>== fun osd_id ->

  Lwt_log.debug_f
    "object (%S, %S) chunk %i: fetching fragment %i"
    object_id object_name
    chunk_id fragment_id
  >>= fun () ->

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
                        device_client # get_option
                                      (osd_access # get_default_osd_priority)
                                      key
                        >>= E.return))
           (let open Asd_protocol.Protocol in
            function
            | Error.Exn err -> E.fail (`AsdError err)
            | exn -> E.fail (`AsdExn exn))
         >>== function
         | None ->
            let msg =
              Printf.sprintf
                "Detected missing fragment namespace_id=%li object_name=%S object_id=%S osd_id=%li (chunk,fragment,version)=(%i,%i,%i)"
                namespace_id object_name object_id osd_id
                chunk_id fragment_id version_id
            in
            Lwt_log.warning msg >>= fun () ->
            E.fail `FragmentMissing
         | Some data ->
            osd_access # get_osd_info ~osd_id >>= fun (_, state) ->
            Osd_state.add_read state;
            fragment_cache # add
                           namespace_id key_string
                           data >>= fun () ->
            let hit_or_mis = false in
            E.return (osd_id, hit_or_mis, data)
       end
    | Some data ->
       let hit_or_mis = true in
       E.return (osd_id, hit_or_mis, data)
  in
  retrieve key

(* consumers of this method are responsible for freeing
 * the returned fragment bigstring
 *)
let download_fragment
      (osd_access : osd_access)
      ~location
      ~namespace ~namespace_id
      ~object_id ~object_name
      ~chunk_id ~fragment_id
      ~replication
      ~fragment_checksum
      decompress
      ~encryption
      (fragment_cache : Fragment_cache.cache)
  =

  let t0_fragment = Unix.gettimeofday () in

  E.with_timing
    (fun () ->
     download_packed_fragment
       osd_access
       ~location
       ~namespace_id
       ~object_id ~object_name
       ~chunk_id ~fragment_id
       fragment_cache)
  >>== fun (t_retrieve, (osd_id, hit_or_miss, fragment_data)) ->

  E.with_timing
    (fun () ->
     Fragment_helper.verify fragment_data fragment_checksum
     >>= E.return)
  >>== fun (t_verify, checksum_valid) ->

  (if checksum_valid
   then E.return ()
   else
     begin
       Lwt_bytes.unsafe_destroy fragment_data;
       E.fail `ChecksumMismatch
     end) >>== fun () ->

  E.with_timing
    (fun () ->
     Fragment_helper.maybe_decrypt
       encryption
       ~namespace
       ~object_id ~chunk_id ~fragment_id
       ~ignore_fragment_id:replication
       fragment_data
     >>= E.return)
  >>== fun (t_decrypt, maybe_decrypted) ->

  E.with_timing
    (fun () ->
     decompress maybe_decrypted
     >>= E.return)
  >>== fun (t_decompress, (maybe_decompressed : Lwt_bytes.t)) ->

  let t_fragment = Statistics.({
                                  osd_id;
                                  retrieve = t_retrieve;
                                  hit_or_miss;
                                  verify = t_verify;
                                  decrypt = t_decrypt;
                                  decompress = t_decompress;
                                  total = Unix.gettimeofday () -. t0_fragment;
                                }) in

  E.return (t_fragment, maybe_decompressed)
