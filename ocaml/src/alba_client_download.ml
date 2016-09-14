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
open Slice
open Alba_statistics
open Alba_client_errors
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
     osd_access # get_osd_info ~osd_id >>= fun (_, state,_) ->
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

(* consumers of this method are responsible for freeing
 * the returned fragment bigstring
 *)
let download_fragment'
      osd_access
      ~location
      ~namespace_id
      ~object_id ~object_name
      ~chunk_id ~fragment_id
      ~replication
      ~fragment_checksum
      decompress
      ~encryption
      fragment_cache
      ~cache_on_read
      bad_fragment_callback
  =
  download_fragment
    osd_access
    ~location
    ~namespace_id
    ~object_id ~object_name
    ~chunk_id ~fragment_id
    ~replication
    ~fragment_checksum
    decompress
    ~encryption
    fragment_cache
    ~cache_on_read
  >>= function
  | Prelude.Error.Ok a -> Lwt.return a
  | Prelude.Error.Error x ->
     bad_fragment_callback
       ~namespace_id ~object_name ~object_id
       ~chunk_id ~fragment_id ~location;
     match x with
     | `AsdError err -> Lwt.fail (Asd_protocol.Protocol.Error.Exn err)
     | `AsdExn exn -> Lwt.fail exn
     | `NoneOsd -> Lwt.fail_with "can't download fragment from None osd"
     | `FragmentMissing -> Lwt.fail_with "missing fragment"
     | `ChecksumMismatch -> Lwt.fail_with "checksum mismatch"


(* consumers of this method are responsible for freeing
 * the returned fragment bigstrings
 *)
let download_chunk
      ~namespace_id
      ~object_id ~object_name
      chunk_locations ~chunk_id
      decompress
      ~encryption
      k m w'
      osd_access
      fragment_cache
      ~cache_on_read
      bad_fragment_callback
  =

  let t0_chunk = Unix.gettimeofday () in

  let n = k + m in
  let fragments = Hashtbl.create n in

  let module CountDownLatch = Lwt_extra2.CountDownLatch in
  let successes = CountDownLatch.create ~count:k in
  let failures = CountDownLatch.create ~count:(m+1) in
  let finito = ref false in

  let threads : unit Lwt.t list =
    List.mapi
      (fun fragment_id (location, fragment_checksum) ->
        let t =
          Lwt.catch
            (fun () ->
              download_fragment'
                osd_access
                ~namespace_id
                ~location
                ~object_id
                ~object_name
                ~chunk_id
                ~fragment_id
                ~replication:(k=1)
                ~fragment_checksum
                decompress
                ~encryption
                fragment_cache
                ~cache_on_read
                bad_fragment_callback
              >>= fun ((t_fragment, fragment_data) as r) ->

              if !finito
              then
                Lwt_bytes.unsafe_destroy fragment_data
              else
                begin
                  Hashtbl.add fragments fragment_id r;
                  CountDownLatch.count_down successes;
                end;
              Lwt.return ())
            (function
             | Lwt.Canceled -> Lwt.return ()
             | exn ->
                Lwt_log.debug_f
                  ~exn
                  "Downloading fragment %i failed"
                  fragment_id >>= fun () ->
                CountDownLatch.count_down failures;
                Lwt.return ()) in
        Lwt.ignore_result t;
        t)
      chunk_locations in

  ignore threads;

  Lwt.pick [ CountDownLatch.await successes;
             CountDownLatch.await failures; ] >>= fun () ->

  finito := true;

  let () =
    if Hashtbl.length fragments < k
    then
      let () =
        Lwt_log.ign_warning_f
          "could not receive enough fragments for namespace %li, object %S (%S) chunk %i; got %i while %i needed"
          namespace_id
          object_name object_id
          chunk_id (Hashtbl.length fragments) k
      in
      Hashtbl.iter
        (fun _ (_, fragment) -> Lwt_bytes.unsafe_destroy fragment)
        fragments;

      Error.failwith Error.NotEnoughFragments
  in
  let fragment_size =
    let _, (_, bs) = Hashtbl.choose fragments |> Option.get_some in
    Lwt_bytes.length bs
  in

  let rec gather_fragments end_fragment acc_fragments erasures cnt = function
    | fragment_id when fragment_id = end_fragment -> acc_fragments, erasures, cnt
    | fragment_id ->
       let fragment_bigarray, erasures', cnt' =
         if Hashtbl.mem fragments fragment_id
         then snd (Hashtbl.find fragments fragment_id), erasures, cnt + 1
         else Lwt_bytes.create fragment_size, fragment_id :: erasures, cnt in
       if Lwt_bytes.length fragment_bigarray <> fragment_size
       then failwith (Printf.sprintf "fragment %i,%i has size %i while %i expected\n%!" chunk_id fragment_id (Lwt_bytes.length fragment_bigarray) fragment_size);
       gather_fragments
         end_fragment
         (fragment_bigarray :: acc_fragments)
         erasures'
         cnt'
         (fragment_id + 1) in

  let t0_gather_decode = Unix.gettimeofday () in
  let data_fragments_rev, erasures_rev, cnt = gather_fragments k [] [] 0 0 in
  let coding_fragments_rev, erasures_rev', cnt = gather_fragments n [] erasures_rev cnt k in

  let data_fragments = List.rev data_fragments_rev in
  let coding_fragments = List.rev coding_fragments_rev in


  let erasures = List.rev (-1 :: erasures_rev') in

  Lwt_log.ign_debug_f
    "erasures = %s"
    ([%show: int list] erasures);

  Erasure.decode
    ~k ~m ~w:w'
    erasures
    data_fragments
    coding_fragments
    fragment_size >>= fun () ->

  let t_now = Unix.gettimeofday () in

  let t_fragments =
    Hashtbl.fold
      (fun _ (t_fragment,_) acc ->
        t_fragment :: acc)
      fragments
      []
  in

  let t_chunk = Statistics.({
                               gather_decode = t_now -. t0_gather_decode;
                               total = t_now -. t0_chunk;
                               fragments = t_fragments;
                }) in

  Lwt.return (data_fragments, coding_fragments, t_chunk)
