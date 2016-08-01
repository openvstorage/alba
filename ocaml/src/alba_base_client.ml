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
open Lwt.Infix
open Checksum
open Lwt_bytes2
open Alba_statistics
open Fragment_cache
open Alba_interval
open Alba_client_errors
module Osd_sec = Osd
open Nsm_host_access


let default_buffer_pool = Buffer_pool.default_buffer_pool

class client
    (fragment_cache : cache)
    ~(mgr_access : Albamgr_client.client)
    ~(osd_access : Osd_access_type.t)
    ~manifest_cache_size
    ~bad_fragment_callback
    ~nsm_host_connection_pool_size
    ~tls_config
    ~tcp_keepalive
    ~use_fadvise
    ~partial_osd_read
    ~cache_on_read ~cache_on_write
    ~populate_osds_info_cache
  =
  let () =
    if populate_osds_info_cache
    then osd_access # populate_osds_info_cache
         |> Lwt.ignore_result
  in
  let () = Lwt_log.ign_debug_f "client: tls_config:%s" ([%show : Tls.t option] tls_config) in
  let nsm_host_access =
    new nsm_host_access
        mgr_access
        nsm_host_connection_pool_size
        ~tls_config
        default_buffer_pool
        ~tcp_keepalive
  in

  let with_osd_from_pool ~osd_id f = osd_access # with_osd ~osd_id f in

  let get_namespace_osds_info_cache ~namespace_id =
    nsm_host_access # get_namespace_info ~namespace_id >>= fun (_, osds, _) ->
    osd_access # osds_to_osds_info_cache osds
  in
  let osd_msg_delivery_threads = Hashtbl.create 3 in
  let preset_cache = new Alba_client_preset_cache.preset_cache mgr_access in
  let get_preset_info = preset_cache # get in
  let manifest_cache = Manifest_cache.ManifestCache.make manifest_cache_size in
  let bad_fragment_callback
        self
        ~namespace_id ~object_id ~object_name
        ~chunk_id ~fragment_id ~location =
    Manifest_cache.ManifestCache.remove
      manifest_cache
      namespace_id object_name;
    bad_fragment_callback
      self
      ~namespace_id ~object_id ~object_name
      ~chunk_id ~fragment_id ~location
  in
  object(self)

    method get_manifest_cache : (string, string) Manifest_cache.ManifestCache.t = manifest_cache
    method get_fragment_cache = fragment_cache

    method tls_config = tls_config
    method tcp_keepalive = tcp_keepalive

    method mgr_access = mgr_access
    method nsm_host_access = nsm_host_access
    method osd_access = osd_access

    method get_preset_cache = preset_cache
    method get_preset_info = get_preset_info

    method get_ns_preset_info ~namespace_id =
      let open Albamgr_protocol.Protocol in
      nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
      get_preset_info ~preset_name:ns_info.Namespace.preset_name

    method get_namespace_osds_info_cache = get_namespace_osds_info_cache

    method get_cache_on_read_write = cache_on_read, cache_on_write

    method discover_osds ?check_claimed ?check_claimed_delay () : unit Lwt.t =
      Discovery.discovery
        (fun d ->
           Lwt_extra2.ignore_errors
             (fun () -> osd_access # seen ?check_claimed ?check_claimed_delay d))

    method with_osd :
      'a. osd_id : Albamgr_protocol.Protocol.Osd.id ->
      (Osd.osd -> 'a Lwt.t) -> 'a Lwt.t =
      fun ~osd_id f ->
        with_osd_from_pool ~osd_id f

    method with_nsm_client' :
      'a. namespace_id : int32 ->
      (Nsm_client.client -> 'a Lwt.t) -> 'a Lwt.t =
      fun ~namespace_id f ->
        nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->
        f nsm

    method with_nsm_client :
      'a. namespace : string ->
      (Nsm_client.client -> 'a Lwt.t) -> 'a Lwt.t = nsm_host_access # with_nsm_client

    method deliver_messages_to_most_osds ~osds ~preset =
      Alba_client_message_delivery.deliver_messages_to_most_osds
        mgr_access nsm_host_access osd_access
        osd_msg_delivery_threads
        ~osds ~preset

    method get_object_manifest' =
      Alba_client_download.get_object_manifest'
        nsm_host_access
        manifest_cache

    method upload_object_from_file
      ~namespace
      ~object_name
      ~input_file
      ~checksum_o
      ~allow_overwrite
      =
      Lwt_log.debug_f
        "Uploading object %S (namespace=%S) from file %s"
        object_name
        namespace
        input_file >>= fun () ->

      Lwt.catch
        (fun () ->
          Object_reader.with_file_reader
            ~use_fadvise
            input_file
            (self # upload_object
                  ~namespace
                  ~object_name
                  ~checksum_o
                  ~allow_overwrite
                  ~object_id_hint:None
            )
        )
        (function
          | Unix.Unix_error(Unix.ENOENT,_,y) ->
             let open Error in failwith FileNotFound
          | (Error.Exn e) as exn ->
             Lwt_log.info_f ~exn "%s" (Error.show e) >>= fun () ->
             Lwt.fail exn
          | exn ->
             Lwt_log.info_f ~exn "generic propagation ..." >>= fun () ->
             Lwt.fail exn
        )

    method upload_object_from_bytes
        ~namespace
        ~(object_name : string)
        ~(object_data : Lwt_bytes.t)
        ~(checksum_o: Checksum.t option)
        ~(allow_overwrite : Nsm_model.overwrite)
      =
      let object_reader = new Object_reader.bytes_reader object_data in

      self # upload_object
        ~namespace
        ~object_name
        ~object_reader
        ~checksum_o
        ~allow_overwrite
        ~object_id_hint:None

    method upload_object_from_bigstring_slice
        ~namespace
        ~(object_name : string)
        ~(object_data : Bigstring_slice.t)
        ~(checksum_o: Checksum.t option)
        ~(allow_overwrite : Nsm_model.overwrite)
      =
      let object_reader = new Object_reader.bigstring_slice_reader object_data in

      self # upload_object
           ~namespace
           ~object_name
           ~object_reader
           ~checksum_o
           ~allow_overwrite
           ~object_id_hint:None

    method upload_object_from_string
      ~namespace
      ~object_name
      ~object_data
      ~checksum_o
      ~allow_overwrite =
      self # upload_object
        ~namespace
        ~object_name
        ~object_reader:(new Object_reader.string_reader object_data)
        ~checksum_o
        ~allow_overwrite
        ~object_id_hint:None

    method upload_object
        ~(namespace : string)
        ~(object_name : string)
        ~(object_reader : Object_reader.reader)
        ~(checksum_o: Checksum.t option)
        ~(allow_overwrite : Nsm_model.overwrite)
        ~(object_id_hint: string option)
      =
      nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->
           self # upload_object'
             ~namespace_id
             ~object_name
             ~object_reader
             ~checksum_o
             ~allow_overwrite
             ~object_id_hint
        )

    method upload_object'
             ~namespace_id
             ~object_name
             ~object_reader
             ~checksum_o
             ~allow_overwrite
             ~object_id_hint
      =
       Alba_client_upload.upload_object'
         nsm_host_access osd_access
         manifest_cache
         get_preset_info
         get_namespace_osds_info_cache
         ~namespace_id
         ~object_name
         ~object_reader
         ~checksum_o
         ~allow_overwrite
         ~object_id_hint
         ~fragment_cache
         ~cache_on_write

    (* consumers of this method are responsible for freeing
     * the returned fragment bigstring
     *)
    method download_fragment
        ~location
        ~namespace_id
        ~object_id ~object_name
        ~chunk_id ~fragment_id
        ~replication
        ~fragment_checksum
        decompress
        ~encryption =
      Alba_client_download.download_fragment
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
           self
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
    method download_chunk
        ~namespace_id
        ~object_id ~object_name
        chunk_locations ~chunk_id
        decompress
        ~encryption
        k m w' =

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
                    self # download_fragment
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

    method download_object_slices
      ~namespace
      ~object_name
      ~(object_slices : (int64 * int * Lwt_bytes.t * int) list)
      ~consistent_read
      ~fragment_statistics_cb
      =
      Lwt_log.debug_f "download_object_slices: %S %S %s consistent_read:%b"
                      namespace object_name
                      ([%show: (int64 * int) list]
                         (List.map
                            (fun (offset, length, _, _) -> offset, length)
                            object_slices))
                      consistent_read
      >>= fun () ->
      nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->
           self # download_object_slices'
             ~namespace_id
             ~object_name
             ~object_slices
             ~consistent_read
             ~fragment_statistics_cb
        )

    method download_object_slices''
      ~namespace_id
      ~manifest
      ~(object_slices : (Int64.t * int * Lwt_bytes.t * int) list)
      ~fragment_statistics_cb
      =
      let open Nsm_model in

      let object_name = manifest.Manifest.name in

      let slices =
        List.sort
          (fun (off1, _, _, _) (off2, _, _, _) -> compare off1 off2)
          object_slices
        |> List.map
             (fun (offset, length, dest, dest_off) ->
              Interval.({ offset; length; }), dest, dest_off)
      in

      let last_slice, length =
        (* ensuring our client doesn't ask stupid things *)
        List.fold_left
          (fun (prev, acc_length) (object_slice, _, _) ->
           let open Interval in

           if object_slice.length < 0
           then Error.(failwith BadSliceLength);

           if overlap prev object_slice
           then Error.(failwith OverlappingSlices);

           object_slice, Int64.(add acc_length (of_int object_slice.length)))
          (Interval.({ offset = -1L; length = 0; }), 0L)
          slices
      in

      assert Int64.(length <: of_int (2 lsl 31));

      (* length sanity check (this also filters out an empty slices list) *)
      if length = 0L
      then Lwt.return (Some manifest)
      else if Int64.(manifest.Manifest.size <:
                       add last_slice.Interval.offset (of_int last_slice.Interval.length))
      then Error.(failwith SliceOutsideObject)
      else
        begin

          let object_id = manifest.Manifest.object_id in
          let es, compression = match manifest.Manifest.storage_scheme with
            | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
          let enc = manifest.Manifest.encrypt_info in
          let decompress = Fragment_helper.maybe_decompress compression in
          let k, m, w = match es with
            | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
          let w' = Encoding_scheme.w_as_int w in
          let locations = manifest.Manifest.fragment_locations in
          let fragment_checksums = manifest.Manifest.fragment_checksums in
          let fragment_info =
            Layout.combine
              locations
              fragment_checksums
          in

          let open Albamgr_protocol.Protocol in
          nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
          get_preset_info ~preset_name:ns_info.Namespace.preset_name >>= fun preset ->
          let encryption = Encrypt_info_helper.get_encryption preset enc in

          let _, _, intersections =
            List.fold_left
              (fun (chunk_offset, chunk_id, acc) chunk_size ->
               let fragment_size = chunk_size / k in

               let rec inner acc total_offset = function
                 | fragment_id when fragment_id = k -> acc
                 | fragment_id ->
                    let fragment_slice = Interval.({ offset = total_offset;
                                                     length = fragment_size; }) in
                    let intersecting_slices =
                      List.fold_left
                        (fun acc (slice, dest, dest_off) ->
                         let open Interval in
                         (* TODO all slices are compared with all fragment intervals?
                          * that's not really awesome...
                          * could do sth similar to merging 2 sorted lists
                          *)
                         match intersection fragment_slice slice with
                         | None -> acc
                         | Some fragment_slice_intersection ->
                            let fragment_offset =
                              let open Int64 in
                              sub
                                fragment_slice_intersection.offset
                                total_offset
                              |> to_int
                            in
                            let dest_off =
                              dest_off +
                                (let open Int64 in
                                 sub
                                   fragment_slice_intersection.offset
                                   slice.offset
                                 |> to_int)
                            in
                            (fragment_offset, fragment_slice_intersection.length,
                             dest, dest_off) :: acc)
                        []
                        slices
                    in

                    let acc' =
                      if intersecting_slices = []
                      then acc
                      else (fragment_id, fragment_slice, intersecting_slices) :: acc
                    in

                    inner
                      acc'
                      Int64.(add total_offset (of_int fragment_size))
                      (fragment_id + 1)
               in
               let chunk_intersections = inner [] chunk_offset 0 in

               let acc' =
                 if chunk_intersections = []
                 then acc
                 else (chunk_id, chunk_intersections) :: acc
               in

               (Int64.(add chunk_offset (of_int chunk_size)), chunk_id + 1, acc'))
              (0L, 0, [])
              manifest.Manifest.chunk_sizes
          in

          Lwt_list.iter_s
            (fun (chunk_id, chunk_intersections) ->
             let chunk_locations = List.nth_exn fragment_info chunk_id in

             let fetch_fragment fragment_id =
               let location, fragment_checksum =
                 List.nth_exn chunk_locations fragment_id
               in
               self # download_fragment
                    ~namespace_id
                    ~object_id ~object_name
                    ~location
                    ~chunk_id ~fragment_id
                    ~replication:(k=1)
                    ~fragment_checksum
                    decompress
                    ~encryption
               >>= fun (t_fragment, fragment_data) ->
               Lwt.catch
                 (fun () ->
                  let () = fragment_statistics_cb t_fragment in
                  Lwt.return fragment_data)
                 (fun exn ->
                  Lwt_bytes.unsafe_destroy fragment_data;
                  Lwt.fail exn)
             in

             let blit_from_fragment fragment_data fragment_intersections =
               List.iter
                 (fun (offset, length,
                       dest, dest_off) ->
                  Lwt_bytes.blit
                    fragment_data
                    offset
                    dest
                    dest_off
                    length)
                 fragment_intersections
             in

             let try_partial_osd_read fragment_id fragment_intersections =
               if partial_osd_read
                  && (let open Nsm_model.Compression in
                      match compression with
                      | NoCompression -> true
                      | Snappy | Bzip2 -> false)
                  && (let open Encryption.Encryption in
                      match encryption with
                      | NoEncryption -> true
                      | AlgoWithKey (AES (CBC, _), _) -> false)
               then
                 begin
                   let (osd_id_o, version_id), fragment_checksum =
                     List.nth_exn chunk_locations fragment_id
                   in
                   match osd_id_o with
                   | None -> Lwt.return_false
                   | Some osd_id ->
                      osd_access # with_osd
                                 ~osd_id
                                 (fun osd ->
                                  let osd_key =
                                    Osd_keys.AlbaInstance.fragment
                                      ~object_id ~version_id
                                      ~chunk_id ~fragment_id
                                    |> Slice.wrap_string
                                  in
                                  (osd # namespace_kvs namespace_id) # partial_get
                                      (osd_access # get_default_osd_priority)
                                      osd_key
                                      fragment_intersections) >>=
                        let open Osd_sec in
                        function
                        | Unsupported -> Lwt.return_false
                        | Success -> Lwt.return_true
                        | NotFound -> Lwt.fail_with "missing fragment"
                 end
               else
                 Lwt.return_false
             in

             let get_from_fragments () =
               Lwt_list.iter_p
                 (fun (fragment_id, fragment_slice, fragment_intersections) ->
                  fragment_cache # lookup2
                                 namespace_id
                                 (Fragment_cache_keys.make_key
                                    ~object_id ~chunk_id ~fragment_id)
                                 fragment_intersections
                  >>= function
                  | true ->
                     (* read fragment pieces from the fragment cache *)
                     Lwt.return_unit
                  | false ->
                     try_partial_osd_read fragment_id fragment_intersections
                     >>= function
                     | true -> Lwt.return_unit
                     | false ->
                        (* partial osd read was not possible or desired,
                         * try reading entire fragment and blit from that *)
                        fetch_fragment fragment_id >>= fun fragment_data ->
                        let () =
                          finalize
                            (fun () -> blit_from_fragment fragment_data fragment_intersections)
                            (fun () -> Lwt_bytes.unsafe_destroy fragment_data)
                        in
                        Lwt.return_unit)
                 chunk_intersections
             in

             let fetch_chunk () =
               self # download_chunk
                    ~namespace_id
                    ~encryption
                    ~object_id
                    ~object_name
                    chunk_locations ~chunk_id
                    decompress
                    k m w'
               >>= fun (data_fragments, coding_fragments, t_chunk) ->
               List.iter
                 Lwt_bytes.unsafe_destroy
                 coding_fragments;
               Lwt.return data_fragments
             in

             let get_from_chunk () =
               fetch_chunk () >>= fun data_fragments ->
               let () =
                 finalize
                   (fun () ->
                    List.iter
                      (fun (fragment_id, fragment_slice, fragment_intersections) ->
                       blit_from_fragment
                         (List.nth_exn data_fragments fragment_id)
                         fragment_intersections)
                      chunk_intersections)
                   (fun () ->
                    List.iter
                      Lwt_bytes.unsafe_destroy
                      data_fragments)
               in
               Lwt.return_unit
             in

             Lwt.catch
               (fun () ->
                Lwt_unix.with_timeout
                  (osd_access # osd_timeout)
                  get_from_fragments)
               (fun exn ->
                Lwt_log.debug_f
                  ~exn
                  "Exception during get_from_fragments, trying get_from_chunk" >>= fun () ->
                get_from_chunk ())
            )
            intersections >>= fun () ->

          Lwt.return (Some manifest)
        end

    method download_object_slices'
             ~namespace_id
             ~object_name
             ~object_slices
             ~consistent_read
             ~fragment_statistics_cb
      =
      let attempt_download_slices manifest (mf_src:Cache.value_source) =
        self # download_object_slices''
             ~namespace_id
             ~manifest
             ~object_slices
             ~fragment_statistics_cb
        >>= function
        | None -> Lwt.return_none
        | Some mf -> Lwt.return (Some (mf, namespace_id, mf_src))

      in
      self # get_object_manifest'
        ~namespace_id ~object_name
        ~consistent_read ~should_cache:true
      >>= fun (mf_src, r) ->
      match r with
      | None -> Lwt.return_none
      | Some manifest ->
         Lwt.catch
           (fun () ->attempt_download_slices manifest mf_src)
           (fun exn ->
            match exn with
            | Error.Exn Error.NotEnoughFragments ->
               begin
                 let open Cache in
                 match mf_src with
                 | Fast ->
                    begin
                      self # get_object_manifest' ~namespace_id ~object_name
                           ~consistent_read:true ~should_cache:true
                      >>= fun (_,r) ->
                      (* Option.map_lwt? *)
                      match r with
                      | Some manifest -> attempt_download_slices manifest Stale
                      | None -> Lwt.return None
                    end
                 | _ -> Lwt.fail exn
               end
             | exn -> Lwt.fail exn
           )



    method download_object_generic''
        ~namespace_id
        ~manifest
        ~get_manifest_dh
        ~t0_object
        ~(write_object_data :
            (Lwt_bytes.t ->
             int ->
             int ->
             unit Lwt.t))
      =
      let open Nsm_model in

      let object_name = manifest.Manifest.name in
      let object_id = manifest.Manifest.object_id in
      let locations = manifest.Manifest.fragment_locations in
      let fragment_checksums = manifest.Manifest.fragment_checksums in
      let fragment_info =
        Layout.combine
          locations
          fragment_checksums
      in
      let es, compression = match manifest.Manifest.storage_scheme with
        | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
      let enc = manifest.Manifest.encrypt_info in
      let decompress = Fragment_helper.maybe_decompress compression in
      let k, m, w = match es with
        | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
      let w' = Encoding_scheme.w_as_int w in

      let open Albamgr_protocol.Protocol in
      nsm_host_access # get_namespace_info ~namespace_id >>= fun (ns_info, _, _) ->
      get_preset_info ~preset_name:ns_info.Namespace.preset_name >>= fun preset ->
      let encryption = Encrypt_info_helper.get_encryption preset enc in

      let open Manifest in

      let checksum = manifest.checksum in
      let hash2 = Hashes.make_hash (Checksum.algo_of checksum) in
      let object_size = Int64.to_int manifest.Manifest.size in

      Lwt_list.fold_left_s
        (fun (offset, t_chunks, t_write_data, t_verify) (chunk_id, chunk_locations) ->
         let chunk_size =
           List.nth_exn manifest.Manifest.chunk_sizes chunk_id
         in
         let fragment_size = chunk_size / k in

         self # download_chunk
              ~namespace_id
              ~object_id
              ~object_name
              chunk_locations
              ~chunk_id
              ~encryption
              decompress
              k m w'
         >>= fun (data_fragments, coding_fragments, t_chunk) ->


         Lwt.finalize
           (fun () ->
            Lwt_list.fold_left_s
              (fun (offset, t_write_data, t_verify) fragment ->
               let fragment_size' =
                 if offset + fragment_size < object_size
                 then fragment_size
                 else (object_size - offset)
               in
               with_timing_lwt
                 (fun () ->
                  hash2 # update_lwt_bytes_detached fragment 0 fragment_size')
               >>= fun (t_verify', ()) ->
               with_timing_lwt
                 (fun () ->
                  write_object_data fragment 0 fragment_size') >>= fun (t_write_data', ()) ->
               Lwt.return (offset + fragment_size',
                           t_write_data +. t_write_data',
                           t_verify +. t_verify'))
              (offset, t_write_data, t_verify)
              data_fragments)
           (fun () ->
            List.iter
              Lwt_bytes.unsafe_destroy
              data_fragments;
            List.iter
              Lwt_bytes.unsafe_destroy
              coding_fragments;
            Lwt.return ())
         >>= fun (offset', t_write_data', t_verify') ->
         Lwt.return (offset',
                     t_chunk :: t_chunks,
                     t_write_data',
                     t_verify'))
        (0, [], 0., 0.)
        (List.mapi (fun i fragment_info -> i, fragment_info) fragment_info)
      >>= fun (_, t_chunks, t_write_data, t_verify) ->
      let t_chunks = List.rev t_chunks in

      let checksum2 = hash2 # final () in

      let t_object = Statistics.({
                                    get_manifest_dh = get_manifest_dh;
                                    chunks = t_chunks;
                                    verify = t_verify;
                                    write_data = t_write_data;
                                    total = Unix.gettimeofday () -. t0_object;
                     }) in

      Lwt_log.debug_f
        ~section:Statistics.section
        "Download object %s with timings %s"
        object_name (Statistics.show_object_download t_object) >>= fun () ->

      if checksum <> checksum2
      then
        begin
          let c2s = [% show: Checksum.t ] in
          Lwt_log.warning_f "checksum: %s <-> %s"
                            (c2s (manifest.checksum))
                            (c2s checksum2) >>= fun () ->
          let msg = Printf.sprintf "failing checksum for: %s" object_name in
          Lwt.fail_with msg
        end
      else
        let r = Some (manifest, t_object) in
        Lwt.return r

    method deliver_nsm_host_messages ~nsm_host_id =
      Alba_client_message_delivery.deliver_nsm_host_messages
        mgr_access nsm_host_access osd_access
        ~nsm_host_id

    method drop_cache_by_id ~global namespace_id =
      Manifest_cache.ManifestCache.drop manifest_cache namespace_id;
      fragment_cache # drop namespace_id ~global

    method drop_cache ~global namespace =
      self # nsm_host_access # with_namespace_id
        ~namespace
        (self # drop_cache_by_id ~global)

    method delete_namespace ~namespace =
      Alba_client_namespace.delete_namespace
        mgr_access nsm_host_access
        (self # deliver_nsm_host_messages)
        (self # drop_cache_by_id ~global:true)
        ~namespace
  end
