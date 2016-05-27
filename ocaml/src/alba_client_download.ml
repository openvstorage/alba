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

open Lwt_bytes2
open Slice
open Alba_statistics
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
      (osd_access : Osd_access_type.t)
      ~location
      ~namespace_id
      ~object_id ~object_name
      ~chunk_id ~fragment_id
  =

  let osd_id_o, version_id = location in

  (match osd_id_o with
   | None -> E.fail `NoneOsd
   | Some osd_id -> E.return osd_id)
  >>== fun osd_id ->

  Lwt_log.debug_f
    "download_packed_fragment: object (%S, %S) chunk %i, fragment %i"
    object_id object_name
    chunk_id fragment_id
  >>= fun () ->

  let osd_key =
    Osd_keys.AlbaInstance.fragment
      ~object_id ~version_id
      ~chunk_id ~fragment_id
    |> Slice.wrap_string
  in

  Lwt.catch
    (fun () ->
     osd_access # with_osd
                ~osd_id
                (fun device_client ->
                 (device_client # namespace_kvs namespace_id) # get_option
                               (osd_access # get_default_osd_priority)
                               osd_key
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
     E.return (osd_id, data)

(* consumers of this method are responsible for freeing
 * the returned fragment bigstring
 *)
let download_fragment
      (osd_access : Osd_access_type.t)
      ~location
      ~namespace_id
      ~object_id ~object_name
      ~chunk_id ~fragment_id
      ~replication
      ~fragment_checksum
      decompress
      ~encryption
      (fragment_cache : Fragment_cache.cache)
      ~cache_on_read
  =

  let t0_fragment = Unix.gettimeofday () in

  let cache_key =
    Fragment_cache_keys.make_key
      ~object_id
      ~chunk_id
      ~fragment_id
  in

  fragment_cache # lookup namespace_id cache_key >>= function
  | Some data ->
     E.return (Statistics.FromCache (Unix.gettimeofday () -. t0_fragment),
               data)
  | None ->
     E.with_timing
       (fun () ->
        download_packed_fragment
          osd_access
          ~location
          ~namespace_id
          ~object_id ~object_name
          ~chunk_id ~fragment_id)
     >>== fun (t_retrieve, (osd_id, fragment_data)) ->

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

     begin
       if cache_on_read
       then
         fragment_cache # add
                        namespace_id
                        cache_key
                        (Bigstring_slice.wrap_bigstring maybe_decompressed)
       else
         Lwt.return_unit
     end >>= fun () ->

     let t_fragment = Statistics.(FromOsd {
                                     osd_id;
                                     retrieve = t_retrieve;
                                     verify = t_verify;
                                     decrypt = t_decrypt;
                                     decompress = t_decompress;
                                     total = Unix.gettimeofday () -. t0_fragment;
                                   }) in

     E.return (t_fragment, maybe_decompressed)
