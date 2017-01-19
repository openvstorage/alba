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
open Lwt.Infix
open Checksum
open Lwt_bytes2
open Alba_statistics
open Fragment_cache
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
    ~use_fadvise
    ~partial_osd_read
    ~cache_on_read ~cache_on_write
    ~populate_osds_info_cache
    ~upload_slack
    ~(read_preference: string list)
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
  in

  let with_osd_from_pool ~osd_id f = osd_access # with_osd ~osd_id f in

  let get_namespace_osds_info_cache ~namespace_id =
    nsm_host_access # get_namespace_info ~namespace_id >>= fun (_, osds, _) ->
    osd_access # osds_to_osds_info_cache osds
  in
  let osd_msg_delivery_threads = Hashtbl.create 3 in
  let preset_cache =
    new Alba_client_preset_cache.preset_cache
        mgr_access
        nsm_host_access
  in
  let get_preset_info = preset_cache # get in
  let manifest_cache = Manifest_cache.ManifestCache.make manifest_cache_size in
  let bad_fragment_callback
        self
        ~namespace_id ~object_name ~object_id
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
    method read_preference = read_preference

    method upload_slack = upload_slack

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
    method get_partial_osd_read = partial_osd_read

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
      'a. namespace_id : int64 ->
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
      ~(epilogue_delay:float option)
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

      Object_reader.with_file_reader
        ~use_fadvise
        input_file
        (self # upload_object
              ~namespace
              ~object_name
              ~checksum_o
              ~allow_overwrite
              ~object_id_hint:None
              ~epilogue_delay
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
        ~(epilogue_delay: float option)
      =
      nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->
          self # upload_object'
             ~epilogue_delay
             ~namespace_id
             ~object_name
             ~object_reader
             ~checksum_o
             ~allow_overwrite
             ~object_id_hint
             ?timestamp:None
        )

    method upload_object'
             ~epilogue_delay
             ~namespace_id
             ~object_name
             ~object_reader
             ~checksum_o
             ~allow_overwrite
             ?timestamp
             ~object_id_hint
      =
       Alba_client_upload.upload_object'
         nsm_host_access osd_access
         manifest_cache
         preset_cache
         get_namespace_osds_info_cache
         ~namespace_id
         ~object_name
         ~object_reader
         ~checksum_o
         ~allow_overwrite
         ~object_id_hint
         ~fragment_cache
         ~cache_on_write
         ~upload_slack
         ~epilogue_delay:None
         ?timestamp


    (* consumers of this method are responsible for freeing
     * the returned fragment bigstrings
     *)
    method download_chunk
        ?(use_bfc = true)
        ~download_strategy
        ~namespace_id
        ~object_id ~object_name
        chunk_locations ~chunk_id
        decompress
        ~encryption
        k m w'

      =
      let bfc =
        if use_bfc
        then Some (bad_fragment_callback self)
        else None
      in
      Alba_client_download.download_chunk
        ~namespace_id
        ~object_id ~object_name
        chunk_locations ~chunk_id
        decompress
        ~encryption
        k m w'
        osd_access
        fragment_cache
        ~cache_on_read
        bfc
        ~download_strategy
        ~read_preference

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


    method download_object_slices'
             ~namespace_id
             ~object_name
             ~object_slices
             ~consistent_read
             ~fragment_statistics_cb
      =
      Alba_client_download_slices.download_object_slices
        mgr_access
        nsm_host_access
        get_preset_info
        manifest_cache
        ~consistent_read
        ~namespace_id
        ~object_name
        ~object_slices
        ~fragment_statistics_cb
        osd_access
        fragment_cache
        ~cache_on_read
        None
        ~partial_osd_read
        ~get_ns_preset_info:(self # get_ns_preset_info)
        ~get_namespace_osds_info_cache
        ~do_repair:true
        ~read_preference

    method download_object_generic''
        ?(use_bfc = true)
        ~namespace_id
        ~manifest
        ~get_manifest_dh
        ~t0_object
        ~(write_object_data :
            (Lwt_bytes.t ->
             int ->
             int ->
             unit Lwt.t))
        ~(download_strategy: Alba_client_download.download_strategy)
      =
      let open Nsm_model in

      let object_name = manifest.Manifest.name in
      let object_id = manifest.Manifest.object_id in
      let fragment_info = Manifest.combined_fragment_infos manifest in
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
              ~use_bfc
              ~namespace_id
              ~object_id
              ~object_name
              chunk_locations
              ~chunk_id
              ~encryption
              decompress
              k m w'
              ~download_strategy
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
        "Download object %S with timings %s"
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
        let r = Some (manifest, t_object, namespace_id) in
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
