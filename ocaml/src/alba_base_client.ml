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
open Lwt
open Checksum
open Slice
open Lwt_bytes2
open Alba_statistics
open Fragment_cache
open Alba_interval
open Alba_client_errors
module Osd_sec = Osd
open Nsm_host_access
open Osd_access

let hm_to_source =  function
  | true  -> Statistics.Cache
  | false -> Statistics.NsmHost


let default_buffer_pool = Buffer_pool.default_buffer_pool

class client
    (fragment_cache : cache)
    ~(mgr_access : Albamgr_client.client)
    ~manifest_cache_size
    ~bad_fragment_callback
    ~nsm_host_connection_pool_size
    ~osd_connection_pool_size
    ~osd_timeout
  =

  let nsm_host_access =
    new nsm_host_access
        mgr_access
        nsm_host_connection_pool_size
        default_buffer_pool
  in

  let osd_access =
    new osd_access mgr_access ~osd_connection_pool_size ~osd_timeout
  in
  let with_osd_from_pool ~osd_id f = osd_access # with_osd ~osd_id f in
  let get_osd_info = osd_access # get_osd_info in

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
        ~chunk_id ~fragment_id ~version_id =
    Manifest_cache.ManifestCache.remove
      manifest_cache
      namespace_id object_name;
    bad_fragment_callback
      self
      ~namespace_id ~object_id ~object_name
      ~chunk_id ~fragment_id ~version_id
  in
  object(self)

    method get_manifest_cache : (string, string) Manifest_cache.ManifestCache.t = manifest_cache
    method get_fragment_cache = fragment_cache

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

    method discover_osds_check_claimed ?check_claimed_delay () : unit Lwt.t =
      Discovery.discovery
        (fun d ->
           Lwt_extra2.ignore_errors
             (fun () -> osd_access # seen ?check_claimed_delay d))

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
           input_file
           (self # upload_object
                ~namespace
                ~object_name
                ~checksum_o
                ~allow_overwrite))
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

    method upload_object
        ~(namespace : string)
        ~(object_name : string)
        ~(object_reader : Object_reader.reader)
        ~(checksum_o: Checksum.t option)
        ~(allow_overwrite : Nsm_model.overwrite)
      =
      nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->
           self # upload_object'
             ~namespace_id
             ~object_name
             ~object_reader
             ~checksum_o
             ~allow_overwrite)

    method upload_object'
             ~namespace_id
             ~object_name
             ~object_reader
             ~checksum_o
             ~allow_overwrite =
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

    (* consumers of this method are responsible for freeing
     * the returned fragment bigstring
     *)
    method download_fragment
        ~osd_id_o
        ~namespace_id
        ~object_id ~object_name
        ~chunk_id ~fragment_id
        ~replication
        ~version_id
        ~fragment_checksum
        decompress
        ~encryption =

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
                       read_it
        >>= function
        | None ->
           begin
             Lwt_log.debug_f "fragment not in cache, trying osd:%li" osd_id
             >>= fun () ->

             Lwt.catch
               (fun () ->
                  with_osd_from_pool
                    ~osd_id
                    (fun device_client ->
                       device_client # get_option key))
               (let open Asd_protocol.Protocol in
                function
                | (Error.Exn err) as exn -> begin match err with
                    | Error.Unknown_error _
                    | Error.ProtocolVersionMismatch _ ->
                      bad_fragment_callback
                        self
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
                 self
                 ~namespace_id ~object_id ~object_name
                 ~chunk_id ~fragment_id ~version_id;
               (* TODO loopke die queue harvest en nr albamgr duwt *)
               (* TODO testje *)
               Lwt.fail_with msg
             | Some (data:Slice.t) ->
               get_osd_info ~osd_id >>= fun (_, state) ->
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
             self
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
          (fun fragment_id ((osd_id_o, version_id), fragment_checksum) ->
             let t =
               Lwt.catch
                 (fun () ->
                    self # download_fragment
                      ~namespace_id
                      ~osd_id_o
                      ~object_id
                      ~object_name
                      ~chunk_id
                      ~fragment_id
                      ~replication:(k=1)
                      ~version_id
                      ~fragment_checksum
                      decompress
                      ~encryption
                    >>= fun (t_fragment, fragment_data) ->

                    if !finito
                    then
                      Lwt_bytes.unsafe_destroy fragment_data
                    else
                      begin
                        Hashtbl.add fragments fragment_id (fragment_data, t_fragment);
                        CountDownLatch.count_down successes;
                      end;
                    Lwt.return ())
                 (function
                   | Canceled -> Lwt.return ()
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
              "could not receive enough fragments for chunk %i; got %i while %i needed\n%!"
              chunk_id (Hashtbl.length fragments) k
          in
          Hashtbl.iter
            (fun _ (fragment, _) -> Lwt_bytes.unsafe_destroy fragment)
            fragments;

          Error.failwith Error.NotEnoughFragments
      in
      let fragment_size =
        let _, (bs, _) = Hashtbl.choose fragments |> Option.get_some in
        Lwt_bytes.length bs
      in

      let rec gather_fragments end_fragment acc_fragments erasures cnt = function
        | fragment_id when fragment_id = end_fragment -> acc_fragments, erasures, cnt
        | fragment_id ->
           let fragment_bigarray, erasures', cnt' =
             if Hashtbl.mem fragments fragment_id
             then fst (Hashtbl.find fragments fragment_id), erasures, cnt + 1
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
          (fun _ (_, t_fragment) acc ->
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
      ~(object_slices : (int64 * int) list)
      ~consistent_read
      write_data =
      Lwt_log.debug_f "download_object_slices: %S %S %s consistent_read:%b"
                      namespace object_name
                      ([%show: (int64 * int) list] object_slices)
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
             write_data)

    method download_object_slices'
      ~namespace_id
      ~object_name
      ~(object_slices : (Int64.t * int) list)
      ~consistent_read
      write_data =
      let attempt_download_slices manifest =
         begin
           let open Nsm_model in

           let slices =
             List.fold_left
               (fun (res_offset, acc) (off, len) ->
                (res_offset + len, (res_offset, off, len) :: acc))
               (0, [])
               object_slices |>
               snd |>
               List.sort (fun (_, off1, _) (_, off2, _) -> compare off1 off2) |>
               List.map
                 (fun (res_offset, offset, length) ->
                  (res_offset,
                   Interval.({ offset; length; })))
           in

           let (res_offset, last_slice), length =
             (* ensuring our client doesn't ask stupid things *)
             List.fold_left
               (fun ((_, prev), acc_length) (res_offset, object_slice) ->
                let open Interval in

                if object_slice.length < 0
                then Error.(failwith BadSliceLength);

                if overlap prev object_slice
                then Error.(failwith OverlappingSlices);

                (res_offset, object_slice), Int64.(add acc_length (of_int object_slice.length)))
               ((0, Interval.({ offset = -1L; length = 0; })), 0L)
               slices
           in

           assert (res_offset < 2 lsl 31);

           (* length sanity check (this also filters out an empty slices list) *)
           if length = 0L
           then Lwt.return (Some manifest)
           else if Int64.(manifest.Manifest.size <:
                            add last_slice.Interval.offset (of_int last_slice.Interval.length))
           then Error.(failwith SliceOutsideObject)
           else begin

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
               let encryption = Preset.get_encryption preset enc in

               let _, _, intersections_rev =
                 List.fold_left
                   (fun (chunk_offset, chunk_id, acc) chunk_size ->
                    let fragment_size = chunk_size / k in

                    let rec inner acc fragment_offset = function
                      | fragment_id when fragment_id = k -> acc
                      | fragment_id ->
                         let fragment_slice = Interval.({ offset = fragment_offset;
                                                          length = fragment_size; }) in
                         let intersecting_slices =
                           List.fold_left
                             (fun acc (res_offset, slice) ->
                              let open Interval in
                              match intersection fragment_slice slice with
                              | None -> acc
                              | Some fragment_slice_intersection ->
                                 let res_offset' =
                                   res_offset +
                                     Int64.(to_int
                                              (sub
                                                 fragment_slice_intersection.offset
                                                 slice.offset)) in
                                 (res_offset', fragment_slice_intersection) :: acc)
                             []
                             slices
                         in

                         let acc' =
                           if intersecting_slices = []
                           then acc
                           else (fragment_id, fragment_slice, List.rev intersecting_slices) :: acc
                         in

                         inner
                           acc'
                           Int64.(add fragment_offset (of_int fragment_size))
                           (fragment_id + 1)
                    in
                    let chunk_intersections = inner [] chunk_offset 0 in

                    let acc' =
                      if chunk_intersections = []
                      then acc
                      else (chunk_id, chunk_offset, chunk_size, List.rev chunk_intersections) :: acc
                    in

                    (Int64.(add chunk_offset (of_int chunk_size)), chunk_id + 1, acc'))
                   (0L, 0, [])
                   manifest.Manifest.chunk_sizes
               in

               let intersections = List.rev intersections_rev in

               Lwt_list.iter_s
                 (fun (chunk_id, chunk_offset, chunk_size, chunk_intersections) ->
                  let chunk_locations = List.nth_exn fragment_info chunk_id in

                  let relevant_fragments =
                    List.fold_left
                      (fun acc (fragment_id, fragment_slice, fragment_intersections) ->
                       IntMap.add
                         fragment_id
                         (fragment_slice, fragment_intersections)
                         acc)
                      IntMap.empty
                      chunk_intersections
                  in

                  let download_fragments () =
                    Lwt_list.map_p
                      (fun (fragment_id, _, _) ->
                         let (osd_id_o, version_id), fragment_checksum =
                           List.nth_exn chunk_locations fragment_id
                         in
                         self # download_fragment
                           ~namespace_id
                           ~object_id ~object_name
                           ~osd_id_o ~version_id
                           ~chunk_id ~fragment_id
                           ~replication:(k=1)
                           ~fragment_checksum
                           decompress
                           ~encryption
                         >>= fun (t_fragment, fragment_data) ->
                         Lwt.return (fragment_id, fragment_data))
                      chunk_intersections
                  in

                  let condition = Lwt_condition.create () in

                  let download_chunk =
                    Lwt_condition.wait condition >>= function
                    | `FragmentsSucceeded ->
                       Lwt.fail Lwt.Canceled
                    | `FragmentsFailed
                    | `Timeout ->
                       self # download_chunk
                            ~namespace_id
                            ~encryption
                            ~object_id
                            ~object_name
                            chunk_locations ~chunk_id
                            decompress
                            k m w' >>= fun (data_fragments, coding_fragments, t_chunk) ->

                       List.iter
                         Lwt_bytes.unsafe_destroy
                         coding_fragments;

                       let data_fragments_i =
                         List.mapi
                           (fun fragment_id fragment_data ->
                            fragment_id, fragment_data)
                           data_fragments in

                       let relevant_fragments_data =
                         List.filter
                           (fun (fragment_id, fragment) ->
                            let b = IntMap.mem fragment_id relevant_fragments in
                            if not b
                            then Lwt_bytes.unsafe_destroy fragment;
                            b)
                           data_fragments_i
                       in

                       Lwt.return relevant_fragments_data
                  in

                  Lwt.ignore_result begin
                      Lwt_unix.sleep 1. >>= fun () ->
                      Lwt_condition.signal condition `Timeout;
                      Lwt.return ()
                    end;

                  let result, wakener = Lwt.wait () in

                  Lwt.async
                    (fun () ->
                     Lwt.catch
                       (fun () ->
                        download_fragments () >>= fun fragments ->
                        let () =
                          try Lwt.wakeup wakener fragments
                          with _ ->
                            List.iter
                              (fun (_, fragment) -> Lwt_bytes.unsafe_destroy fragment)
                              fragments
                        in
                        Lwt.return ())
                       (fun exn ->
                        Lwt_condition.signal condition `FragmentsFailed;
                        Lwt.fail exn)
                    );
                  Lwt.async
                    (fun () ->
                     Lwt.catch
                       (fun () ->
                        download_chunk >>= fun fragments ->
                        let () =
                          try Lwt.wakeup wakener fragments
                          with _ ->
                            List.iter
                              (fun (_, fragment) -> Lwt_bytes.unsafe_destroy fragment)
                              fragments
                        in
                        Lwt.return ())
                       (fun exn ->
                        let () =
                          try Lwt.wakeup_exn wakener exn
                          with _ -> ()
                        in
                        Lwt.return ())
                    );

                  result >>= fun fragments_data ->
                  Lwt_condition.signal condition `FragmentsSucceeded;

                  (* write all data for this chunk *)
                  Lwt_list.iter_s
                    (fun (fragment_id, fragment_data) ->
                     let fragment_slice, fragment_intersections =
                       IntMap.find fragment_id relevant_fragments
                     in
                     Lwt_list.iter_s
                       (fun (res_offset, fragment_intersection) ->
                        let open Interval in
                        let off =
                          let open Int64 in
                          to_int (sub fragment_intersection.offset fragment_slice.offset) in

                        write_data
                          res_offset
                          fragment_data
                          off
                          fragment_intersection.length)
                       fragment_intersections)
                    fragments_data
                 )
                 intersections >>= fun () ->

               Lwt.return (Some manifest)
             end
         end
      in
      self # get_object_manifest'
        ~namespace_id ~object_name
        ~consistent_read ~should_cache:true
      >>= fun (cache_hit, r) ->
      match r with
      | None -> Lwt.return None
      | Some manifest ->
         Lwt.catch
           (fun () ->attempt_download_slices manifest)
           (fun exn ->
            match exn with
             | Error.Exn Error.NotEnoughFragments ->
                if cache_hit
                then
                  begin
                    self # get_object_manifest' ~namespace_id ~object_name
                         ~consistent_read:true ~should_cache:true
                    >>= fun (_,r) ->
                    (* Option.map_lwt? *)
                    match r with
                    | Some manifest -> attempt_download_slices manifest
                    | None -> Lwt.return None
                  end
                else Lwt.fail exn
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
      let encryption = Preset.get_encryption preset enc in

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
               Statistics.with_timing_lwt
                 (fun () ->
                  hash2 # update_lwt_bytes_detached fragment 0 fragment_size')
               >>= fun (t_verify', ()) ->
               Statistics.with_timing_lwt
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

  end
