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

open Lwt.Infix
open Prelude
open Proxy_protocol
open Range_query_args
open Lwt_bytes2

let write_albamgr_cfg albamgr_cfg =
  let value = Arakoon_client_config.to_ini albamgr_cfg in
  function
  | Url.File destination ->
     let tmp = destination ^ ".tmp" in
     Lwt_extra2.unlink ~fsync_parent_dir:false  ~may_not_exist:true tmp >>= fun () ->
     Lwt_extra2.with_fd
       tmp
       ~flags:Lwt_unix.([ O_WRONLY; O_CREAT; O_EXCL; ])
       ~perm:0o664
       (fun fd ->
        Lwt_extra2.write_all
          fd
          value 0 (String.length value) >>= fun () ->
        Lwt_unix.fsync fd) >>= fun () ->
     Lwt_extra2.rename ~fsync_parent_dir:true tmp destination
  | Url.Etcd (peers, path) ->
     Arakoon_etcd.store_value peers path value
  | Url.Arakoon { Url.cluster_id; key; ini_location; } ->
     Arakoon_config_url.(retrieve (File ini_location)) >|= Arakoon_client_config.from_ini
     >>= fun ccfg ->
     Client_helper.with_master_client'
       ccfg
       (fun client -> client # set key value)

let read_objects_slices
      (alba_client : Alba_client.alba_client)
      namespace objects_slices ~consistent_read =
  let total_length =
    List.fold_left
      (fun acc (object_name, object_slices) ->
       List.fold_left
         (fun acc (_, slice_length) -> acc + slice_length)
         acc
         object_slices)
      0
      objects_slices
  in
  let res = Lwt_bytes.create total_length in
  Lwt.finalize
    (fun () ->
     let _, objects_slices, n_slices, object_names =
       List.fold_left
         (fun (res_offset, acc, n_slices, object_names) (object_name, object_slices) ->
          let res_offset, object_slices =
            List.fold_left
              (fun (res_offset, object_slices) (slice_offset, slice_length) ->
               res_offset + slice_length,
               (slice_offset, slice_length, res, res_offset) :: object_slices)
              (res_offset, [])
              object_slices
          in
          let object_names' = StringSet.add object_name object_names
          and n_slices' = n_slices + 1
          and acc' = (object_name, object_slices) :: acc
          in

          res_offset, acc', n_slices', object_names'
         )
         (0, [], 0, StringSet.empty)
         objects_slices
     in

     let n_objects = StringSet.cardinal object_names in
     let strategy, logger =
       if n_slices < 8
       then
         Lwt_list.map_p,
         fun n_slices total_length n_objects ->
         Lwt_log.debug_f "%i slices,%i bytes,%i objects => parallel"
                         n_slices total_length n_objects
       else
         Lwt_list.map_s,
         fun n_slices total_length n_objects ->
         Lwt_log.info_f "%i slices,%i bytes,%i objects => sequential"
                        n_slices total_length n_objects
     in
     let fc_hits   = ref 0 in
     let fc_misses = ref 0 in
     let fragment_statistics_cb stat =
       let open Alba_statistics in
       match stat with
       | Statistics.FromCache _ -> incr fc_hits
       | Statistics.FromOsd _   -> incr fc_misses
     in
     logger n_slices total_length n_objects >>= fun () ->
     strategy
       (fun (object_name, object_slices) ->
        alba_client # download_object_slices
                    ~namespace
                    ~object_name
                    ~object_slices
                    ~consistent_read
                    ~fragment_statistics_cb
        >>= function
        | None -> Protocol.Error.failwith Protocol.Error.ObjectDoesNotExist
        | Some (mf, namespace_id, mf_source, mfs) -> Lwt.return (object_name, mf, namespace_id, mf_source, mfs)
       )
       objects_slices
     >>= fun n_mf_src_s ->

     let objects_infos =
       List.map
         (fun (name, mf, namespace_id, mf_source, mfs) ->
           (name, "", (mf, namespace_id))
           :: (List.map
                 (fun (mf, namespace_id, alba) ->
                   mf.Nsm_model.Manifest.name, alba, (mf, namespace_id))
                 mfs)
         )
         n_mf_src_s
       |> List.flatten_unordered
     in
     let mf_sources = List.map (fun (_,_,_,src, _) -> src) n_mf_src_s in
     Lwt.return (Lwt_bytes.to_string res,
                 n_slices, n_objects, mf_sources,
                 !fc_hits, !fc_misses, objects_infos))
    (fun () ->
     Lwt_bytes.unsafe_destroy res;
     Lwt.return ())


let apply_sequence
      (alba_client : Alba_client.alba_client)
      namespace namespace_id
      asserts
      updates
      stats
  =
  let rec with_alba_sequence_updates updates' = function
    | [] ->
       Alba_client_sequence.apply_sequence
         alba_client
         namespace_id
         (Lwt.return asserts)
         updates' >>= fun (mfs, delta) ->
       List.iter
         (let open Alba_client_sequence in
          function
          | UploadObjectFromReader _ ->
             ProxyStatistics.new_upload stats namespace delta
          | DeleteObject _ ->
             ProxyStatistics.new_delete stats namespace delta)
         updates';
       Lwt.return mfs
    | update :: rest ->
       let open Protocol.Update in
       match update with
       | UploadObjectFromFile (object_name, file_name, cs_o) ->
          Object_reader.with_file_reader
            ~use_fadvise:true
            file_name
            (fun ~object_reader ->
              let update' = Alba_client_sequence.UploadObjectFromReader (object_name, object_reader, cs_o) in
              with_alba_sequence_updates
                (update' :: updates')
                rest)
       | UploadObject (object_name, blob, cs_o) ->
          let reader = new Object_reader.bigstring_slice_reader blob in
          let update' = Alba_client_sequence.UploadObjectFromReader (object_name, reader, cs_o) in
          with_alba_sequence_updates
            (update' :: updates')
            rest
       | DeleteObject object_name ->
          with_alba_sequence_updates
            (Alba_client_sequence.DeleteObject object_name :: updates')
            rest
  in
  with_alba_sequence_updates [] updates


let read_objects
      (alba_client : Alba_client.alba_client)
      ~namespace_id
      object_names
      ~should_cache
      ~consistent_read
  =
  alba_client # nsm_host_access # get_nsm_by_id ~namespace_id >>= fun nsm ->

  Alba_client_download.get_object_manifests'
    (alba_client # nsm_host_access)
    (alba_client # get_manifest_cache)
    ~namespace_id ~object_names
    ~consistent_read ~should_cache
  >>= fun r ->
  let t0_object = Unix.gettimeofday () in
  Lwt_list.map_p
    (function
     | (_, None) -> Lwt.return None
     | (dh, Some manifest) ->
        let bs = Lwt_bytes.create (Int64.to_int manifest.Nsm_model.Manifest.size) in
        let offset = ref 0 in
        let write_object_data source pos len =
          Lwt_bytes.blit source pos bs !offset len;
          offset := !offset + len;
          Lwt.return ()
        in
        Lwt.catch
          (fun () ->
            alba_client # get_base_client # download_object_generic''
                        ~namespace_id
                        ~manifest
                        ~get_manifest_dh:(0., dh)
                        ~t0_object
                        ~write_object_data
                        ~download_strategy:Alba_client_download.AllFragments
                        ~use_bfc:true
            >>= function
            | None ->
               let () = Lwt_bytes.unsafe_destroy bs in
               Lwt.return None
            | Some _ ->
               Lwt.return (Some (manifest, Bigstring_slice.wrap_bigstring bs))
          )
          (fun exn ->
            let () = Lwt_bytes.unsafe_destroy bs in
            Lwt.fail exn))
    r


let render_request_args: type i o. (i,o) Protocol.request -> i -> Bytes.t =
  let open Protocol in
  let render_read_object_slices (namespace, objects_slices, consistent_read) =
    Printf.sprintf
      "(%S,%s )"
      namespace
      ([%show : (object_name *
                   (offset * length) list) list ]
         objects_slices)
  in
  function
  | ListNamespaces  -> fun { RangeQueryArgs.first; finc; last; max; reverse} -> "{ }"
  | ListNamespaces2 -> fun { RangeQueryArgs.first; finc; last; max; reverse} -> "{ }"
  | CreateNamespace -> fun (namespace, preset_option) ->
                       Printf.sprintf "(%S,_)" namespace
  | DeleteNamespace -> fun namespace ->
                       Printf.sprintf "(%S)" namespace
  | ReadObjectFs    -> fun (namespace, object_name, _,_,_ ) ->
                       Printf.sprintf "(%S,%S,_,_,_)" namespace object_name
  | WriteObjectFs   -> fun (namespace, object_name, _,_,_)  ->
                       Printf.sprintf "(%S,%S,_,_,_)" namespace object_name
  | WriteObjectFs2  -> fun (namespace, object_name, _,_,_) ->
                       Printf.sprintf "(%S,%S,_,_,_)" namespace object_name
  | ReadObjectsSlices  -> render_read_object_slices
  | ReadObjectsSlices2 -> render_read_object_slices
  | NamespaceExists -> fun namespace ->
                       Printf.sprintf "(%S)" namespace
  | ProxyStatistics -> fun _ -> "-"
  | DropCache       -> fun _ -> "-"
  | InvalidateCache -> fun _ -> "-"
  | GetObjectInfo   -> fun _ -> "-"
  | ListObjects     -> fun _ -> "-"
  | DeleteObject    -> fun _ -> "_"
  | GetVersion      -> fun _ -> "-"
  | OsdView         -> fun _ -> "-"
  | GetClientConfig -> fun () -> "()"
  | Ping            -> fun delay -> Printf.sprintf "(%f)" delay
  | OsdInfo         -> fun () -> "()"
  | OsdInfo2        -> fun () -> "()"
  | ApplySequence   -> fun (namespace, write_barrier, asserts, updates) ->
                       Printf.sprintf "(%S,%b,%s,%s)"
                                      namespace
                                      write_barrier
                                      ([%show : Assert.t list] asserts)
                                      ([%show : Update.t list] updates)
  | ReadObjects     -> fun args ->
                       Printf.sprintf "(%S)" ([%show : Namespace.name
                                                       * object_name list
                                                       * consistent_read
                                                       * should_cache]
                                                args)
  | MultiExists     -> fun args ->
                       Printf.sprintf "(%S)" ([%show : Namespace.name * object_name list] args)
  | GetAlbaId       -> fun () -> "()"

let log_request code maybe_renderer time =
  let log = if time < 0.5 then  Lwt_log.debug_f else Lwt_log.info_f in
  let details =
    match maybe_renderer with
    | None -> ""
    | Some renderer -> renderer ()
  in
  log "Request %s %s took %f" (Protocol.code_to_txt code) details time


let proxy_protocol (alba_client : Alba_client.alba_client)
                   (albamgr_client_cfg:Alba_arakoon.Config.t ref)
                   (stats: ProxyStatistics.t')
                   (nfd:Net_fd.t) =

  let execute_request : type i o. (i, o) Protocol.request ->
                             ProxyStatistics.t' ->
                             i -> o Lwt.t
                          =
    let open Protocol in
    let write_object_fs stats (namespace,
                               object_name,
                               input_file,
                               allow_overwrite,
                               checksum_o)
      =
      let open Nsm_model in
      Lwt.catch
        (fun () ->
          alba_client # upload_object_from_file
             ~epilogue_delay:None
             ~namespace
             ~object_name ~input_file
             ~checksum_o
             ~allow_overwrite:(if allow_overwrite
                               then Unconditionally
                               else NoPrevious)
           >>= fun (mf , fc_info, upload_stats, namespace_id) ->
           let open Alba_statistics.Statistics in
           ProxyStatistics.new_upload stats namespace upload_stats.total;
           Lwt.return (mf, namespace_id, fc_info )
        )
        (let open Alba_client_errors.Error in
          function
          | Err.Nsm_exn (Err.Overwrite_not_allowed, _) ->
            Protocol.Error.failwith Protocol.Error.OverwriteNotAllowed
          | Exn FileNotFound ->
             Protocol.Error.failwith Protocol.Error.FileNotFound
          | exn -> Lwt.fail exn)
    in
    function
    | ListNamespaces -> fun stats { RangeQueryArgs.first; finc; last; max; reverse } ->
      (* TODO only return namespaces which are active? hmm, maybe creating too... *)
      alba_client # mgr_access # list_namespaces
        ~first ~finc ~last
        ~max ~reverse >>= fun ((cnt, namespaces), has_more) ->
      Lwt.return ((cnt, List.map fst namespaces), has_more)
    | ListNamespaces2 -> fun stats { RangeQueryArgs.first; finc; last; max; reverse } ->
      (* TODO only return namespaces which are active? hmm, maybe creating too... *)
      alba_client # mgr_access # list_namespaces
        ~first ~finc ~last
        ~max ~reverse >>= fun ((cnt, namespaces), has_more) ->
      Lwt.return ((cnt,
                   List.map (fun (name, info) -> name, info.Namespace.preset_name) namespaces),
                  has_more)
    | NamespaceExists -> fun stats namespace ->
      (* TODO keep namespace state in mind? *)
      alba_client # mgr_access # get_namespace ~namespace >>= fun r ->
      Lwt.return (r <> None)
    | CreateNamespace -> fun stats (namespace, preset_name) ->
      alba_client # create_namespace ~preset_name ~namespace () >>= fun namespace_id ->
      Lwt.return ()
    | DeleteNamespace -> fun stats namespace ->
      alba_client # delete_namespace ~namespace
    | ListObjects -> fun stats (namespace,
                          { RangeQueryArgs.first; finc;
                            last; max; reverse }) ->
      alba_client # list_objects ~namespace ~first ~finc ~last ~max ~reverse
    | ReadObjectFs ->
       fun stats (namespace,
                  object_name,
                  output_file,
                  consistent_read,
                  should_cache) ->
       begin
         alba_client # download_object_to_file
                     ~namespace
                     ~object_name
                     ~output_file
                     ~consistent_read ~should_cache
         >>= function
         | None -> Protocol.Error.failwith Protocol.Error.ObjectDoesNotExist
         | Some (mf,download_stats, _) ->
            let open Alba_statistics.Statistics in
            let (_mf_duration, mf_hm) = download_stats.get_manifest_dh in
            let fg_hm = summed_fragment_hit_misses download_stats in
            ProxyStatistics.new_download
              stats namespace
              download_stats.total
              mf_hm
              fg_hm;
            Lwt.return ()
       end
    | WriteObjectFs -> fun stats args ->
                       write_object_fs stats args
                       >>= fun (_mf, _namespace_id, _fc_info) ->
                       Lwt.return_unit
    | WriteObjectFs2 -> fun stats args  ->
                        write_object_fs stats args
                        >>= fun (mf, namespace_id, _fc_info) ->
                        Lwt.return (mf, namespace_id)
    | DeleteObject -> fun stats (namespace, object_name, may_not_exist) ->
      Lwt.catch
        (fun () ->
          with_timing_lwt
           (fun () ->
             alba_client # delete_object ~namespace ~object_name ~may_not_exist)
          >>= fun (delta, ()) ->
          let () = ProxyStatistics.new_delete stats namespace delta in
          Lwt.return_unit
        )
        (function
          | Nsm_model.Err.Nsm_exn (Nsm_model.Err.Overwrite_not_allowed, _) ->
            Protocol.Error.failwith Protocol.Error.ObjectDoesNotExist
          | exn -> Lwt.fail exn)
    | ReadObjects ->
       fun stats (namespace, objs, consistent_read, should_cache) ->
       alba_client # nsm_host_access # with_namespace_id
                   ~namespace
                   (fun namespace_id ->
                     read_objects
                       alba_client
                       ~namespace_id
                       objs
                       ~should_cache
                       ~consistent_read >>= fun values ->
                     Lwt.return (namespace_id, values))
    | MultiExists ->
       fun stats (namespace, object_names) ->
       alba_client # get_base_client # with_nsm_client
                   ~namespace
                   (fun nsm -> nsm # multi_exists object_names)
    | ApplySequence ->
       fun stats (namespace, _write_barrier, asserts, updates) ->
       alba_client # nsm_host_access # with_namespace_id
                   ~namespace
                   (fun namespace_id ->
                     apply_sequence
                       alba_client
                       namespace
                       namespace_id
                       asserts
                       updates
                       stats
                   )
    | GetObjectInfo ->
       fun stats (namespace, object_name, consistent_read, should_cache) ->
       begin
         alba_client # get_object_manifest
           ~namespace ~object_name
           ~consistent_read ~should_cache
         >>= fun (_hit_or_miss, r) ->
         match r with
         | None -> Protocol.Error.failwith Protocol.Error.ObjectDoesNotExist
         | Some manifest ->
           let open Nsm_model.Manifest in
           Lwt.return (manifest.size, manifest.checksum)
       end
    | ReadObjectsSlices ->
       fun stats (namespace, objects_slices, consistent_read) ->
       with_timing_lwt
         (fun () -> read_objects_slices alba_client namespace objects_slices ~consistent_read)
       >>= fun (delay, (bytes, n_slices, n_objects, mf_sources,
                        fc_hits, fc_misses,
                        object_infos)) ->
       let total_length = Bytes.length bytes in
       ProxyStatistics.new_read_objects_slices
         stats namespace
         ~total_length ~n_slices ~n_objects ~mf_sources
         ~fc_hits ~fc_misses
         ~took:delay;
       Lwt.return bytes
    | ReadObjectsSlices2 ->
       fun stats (namespace, objects_slices, consistent_read) ->
       with_timing_lwt
         (fun () -> read_objects_slices alba_client namespace objects_slices ~consistent_read)
       >>= fun (delay, (bytes, n_slices, n_objects,
                        mf_sources, fc_hits, fc_misses,
                        object_infos)) ->
       let total_length = Bytes.length bytes in
       ProxyStatistics.new_read_objects_slices
         stats namespace
         ~total_length ~n_slices ~n_objects ~mf_sources
         ~fc_hits ~fc_misses
         ~took:delay;
       Lwt_log.debug_f "object_infos:%s"
                       ([%show : (string * string * manifest_with_id) list]
                          object_infos)
       >>= fun () ->
       Lwt.return (bytes, object_infos)
    | InvalidateCache ->
      fun stats namespace -> alba_client # invalidate_cache namespace
    | DropCache ->
      fun stats namespace -> alba_client # drop_cache namespace ~global:false
    | ProxyStatistics ->
       fun stats clear ->
       begin
         let stopped = ProxyStatistics.clone stats in
         let () = ProxyStatistics.stop stopped in
         if clear then ProxyStatistics.clear stats;
         Lwt.return stopped
       end
    | GetVersion -> fun stats () -> Lwt.return Alba_version.summary
    | OsdView ->
       fun stats () ->
       let info = alba_client # osd_access # osds_info_cache  in
       let state_info =
         Hashtbl.fold
           (fun osd_id ((osd:Nsm_model.OsdInfo.t),
                        (state:Osd_state.t),
                        (capabilities:Capabilities.OsdCapabilities.t)
                       ) (c,acc) ->
            let c' = c+1
            and acc' = (osd_id, osd, state) :: acc (*TODO: capababilities? *)
            in
            (c',acc')
           )
           info (0,[])
       in
       let claim_info = alba_client # osd_access # get_osd_claim_info in
       let claim_info = (StringMap.cardinal claim_info, StringMap.bindings claim_info) in
       Lwt.return (claim_info, state_info)
    | GetClientConfig ->
       fun stats () ->
       Lwt.return !albamgr_client_cfg
    | Ping ->
       fun stats delay ->
       begin
         Lwt_unix.sleep delay >>= fun () ->
         let t0 = Unix.gettimeofday() in
         Lwt.return t0
       end
    | OsdInfo ->
       fun stats () ->
       begin
         alba_client # osd_access # osd_infos |> Lwt.return
       end
    | OsdInfo2 ->
       fun stats () -> alba_client # osd_infos
    | GetAlbaId ->
       fun stats () -> alba_client # get_alba_id
  in
  let module Llio = Llio2.WriteBuffer in
  let return_err_response ?msg err =
    let res_s =
      Llio.serialize_with_length'
        (Llio.pair_to
           Llio.int_to
           Llio.string_to)
        (Protocol.Error.err2int err,
         (match msg with
          | None -> Protocol.Error.show err
          | Some msg -> msg)) in
    Lwt.return (res_s, None)
  in
  let handle_request buf code : (unit -> string) option Lwt.t =
    Lwt.catch
      (fun () ->
         match Protocol.code_to_command code with
         | Protocol.Wrap r ->
           let req =
             try Deser.from_buffer (Protocol.deser_request_i r) buf
             with exn ->
               Lwt_log.ign_error ~exn "error during deserializing proxy request";
               raise exn
           in
           execute_request r stats req >>= fun res ->
           Lwt.return (Llio.serialize_with_length'
                         (Llio.pair_to
                            Llio.int_to
                            (snd (Protocol.deser_request_o r)))
                         (0, res),
                       let renderer () = render_request_args r req in
                       Some renderer))
      (function
        | End_of_file as e ->
          Lwt.fail e
        | Protocol.Error.Exn (err, payload) ->
           let msg = match payload with
             | Some x -> x
             | None -> Protocol.Error.show err in
           Lwt_log.info_f "Returning %s error to client" msg
           >>= fun () ->
           return_err_response ~msg err
        | Asd_protocol.Protocol.Error.Exn err ->
           let msg = Asd_protocol.Protocol.Error.show err in
           Lwt_log.info_f
             "Unexpected Asd_protocol.Protocol.Error exception in proxy while handling request: %s"
             msg
           >>= fun () ->
           return_err_response ~msg Protocol.Error.Unknown
        | Nsm_model.Err.Nsm_exn (err, payload) ->
          begin
            let open Nsm_model.Err in
            match err with
            | Object_not_found ->
              return_err_response Protocol.Error.ObjectDoesNotExist
            | Assert_failed ->
              return_err_response ~msg:payload Protocol.Error.AssertFailed
            | Namespace_id_not_found
            | Unknown
            | Invalid_gc_epoch
            | Non_unique_object_id
            | Not_master
            | Inconsistent_read
            | InvalidVersionId
            | Old_plugin_version
            | Old_timestamp
            | Invalid_fragment_spread
            | Inactive_osd
            | Invalid_bucket
            | Preset_violated
            | Too_many_disks_per_node
            | Insufficient_fragments
            | Unknown_operation ->
               let msg = Nsm_model.Err.show err in
               Lwt_log.info_f
                 "Unexpected Nsm_model.Err exception in proxy while handling request: %s" msg
               >>= fun () ->
               return_err_response ~msg Protocol.Error.Unknown
            | Overwrite_not_allowed ->
              Lwt_log.info_f
                "Received Nsm_model.Err Overwrite_not_allowed exception in proxy while that should've already been handled earlier..." >>= fun () ->
              return_err_response Protocol.Error.Unknown
          end
        | Albamgr_protocol.Protocol.Error.Albamgr_exn (err, _) ->
          begin
            let open Albamgr_protocol.Protocol.Error in
            match err with
            | Namespace_already_exists ->
              return_err_response Protocol.Error.NamespaceAlreadyExists
            | Namespace_does_not_exist ->
              return_err_response Protocol.Error.NamespaceDoesNotExist
            | Preset_does_not_exist ->
              return_err_response Protocol.Error.PresetDoesNotExist
            | Unknown
            | Osd_already_exists
            | Nsm_host_already_exists
            | Nsm_host_unknown
            | Nsm_host_not_lost
            | Osd_already_linked_to_namespace
            | Not_master
            | Inconsistent_read
            | Old_plugin_version
            | Preset_already_exists
            | Preset_cant_delete_default
            | Preset_cant_delete_in_use
            | Invalid_preset
            | Osd_already_claimed
            | Osd_unknown
            | Osd_info_mismatch
            | Osd_already_decommissioned
            | Claim_lease_mismatch
            | Progress_does_not_exist
            | Progress_CAS_failed
            | Unknown_operation ->
               let msg = Albamgr_protocol.Protocol.Error.show err in
               Lwt_log.info_f
                 "Unexpected Albamgr_protocol.Protocol.Err exception in proxy while handling request: %s"  msg
               >>= fun () ->
               return_err_response ~msg Protocol.Error.Unknown
          end
        | Alba_client_errors.Error.Exn err ->
          begin
            let open Alba_client_errors.Error in
            Lwt_log.info_f "Got error from alba client: %s" (show err) >>= fun () ->
            return_err_response
              (let open Protocol in
               match err with
               | ChecksumMismatch -> Error.ChecksumMismatch
               | ChecksumAlgoNotAllowed -> Error.ChecksumAlgoNotAllowed
               | BadSliceLength -> Error.BadSliceLength
               | OverlappingSlices -> Error.OverlappingSlices
               | SliceOutsideObject -> Error.SliceOutsideObject
               | FileNotFound -> Error.FileNotFound
               | NoSatisfiablePolicy -> Error.NoSatisfiablePolicy
               | NamespaceDoesNotExist -> Error.NamespaceDoesNotExist
               | NotEnoughFragments -> Error.Unknown
              )
          end
        | exn ->
           Lwt_log.info_f ~exn "Unexpected exception in proxy while handling request" >>= fun () ->
           let msg = Printexc.to_string exn in
           return_err_response ~msg Protocol.Error.Unknown)

    >>= fun (res, maybe_renderer) ->
    Net_fd.write_all_lwt_bytes nfd res.Llio.buf 0 res.Llio.pos >>= fun () ->
    Lwt.return maybe_renderer

  in
  let rec inner () =
    Llio2.NetFdReader.int_from nfd >>= fun len ->
    Llio2.NetFdReader.with_buffer_from
      nfd len
      (fun buf ->
       let code = Llio2.ReadBuffer.int_from buf in
       with_timing_lwt
         (fun () -> handle_request buf code)
       >>= fun (time_inner, maybe_renderer) ->
       log_request code maybe_renderer time_inner) >>= fun () ->
    inner ()
  in
  Llio2.NetFdReader.int32_from nfd >>= fun magic ->
  if magic = Protocol.magic
  then
    begin
      Llio2.NetFdReader.int32_from nfd >>= fun version ->
      if version = Protocol.version
      then inner ()
      else
        let err = Protocol.Error.ProtocolVersionMismatch
        and msg = Printf.sprintf
                    "protocol version: (server) %li <> %li (client)"
                    Protocol.version version
        in
        return_err_response ~msg err >>= fun (res, _) ->
        Net_fd.write_all_lwt_bytes nfd res.Llio.buf 0 res.Llio.pos
    end
  else Lwt.return ()



let refresh_albamgr_cfg
    ~loop
    albamgr_client_cfg
    (alba_client : Alba_client.alba_client)
    ~tls_config
    ~tcp_keepalive
    destination =

  let rec inner () =
    Lwt_log.debug "refresh_albamgr_cfg" >>= fun () ->
    let open Albamgr_client in
    Lwt.catch
      (fun () ->
         alba_client # mgr_access # get_client_config
         >>= fun ccfg ->
         Lwt.return (Res ccfg))
      (let open Client_helper.MasterLookupResult in
       function
       | Arakoon_exc.Exception(Arakoon_exc.E_NOT_MASTER, master)
       | Error (Unknown_node (master, (_, _))) ->
          retrieve_cfg_from_any_node ~tls_config !albamgr_client_cfg
       | exn ->
          Lwt_log.debug_f ~exn "refresh_albamgr_cfg failed" >>= fun () ->
          Lwt.return Retry
      )
    >>= function
    | Retry ->
      Lwt_extra2.sleep_approx 60. >>= fun () ->
      inner ()
    | Res ccfg ->
      albamgr_client_cfg := ccfg;
      Lwt.catch
        (fun () -> write_albamgr_cfg ccfg destination)
        (fun exn ->
         Lwt_log.info_f
           ~exn
           "couldn't write config to destination:%s" (Prelude.Url.show destination))
      >>= fun () ->
      Lwt_extra2.sleep_approx 60. >>= fun () ->
      if loop
      then inner ()
      else Lwt.return ()
  in
  inner ()

let run_server hosts port ~transport
               albamgr_client_cfg
               ~fragment_cache
               ~manifest_cache_size
               ~albamgr_connection_pool_size
               ~nsm_host_connection_pool_size
               ~osd_connection_pool_size
               ~osd_timeout
               ~(albamgr_cfg_url: Url.t)
               ~max_client_connections
               ~tls_config
               ~tcp_keepalive
               ~use_fadvise
               ~partial_osd_read
               ~cache_on_read ~cache_on_write
               ~upload_slack
               ~read_preference
  =
  Lwt_log.info_f "proxy_server version:%s" Alba_version.git_revision
  >>= fun () ->
  let stats = ProxyStatistics.make () in

  Lwt.catch
    (fun () ->
       let bad_fragment_callback
           (alba_client : Alba_base_client.client)
           ~namespace_id
           ~object_id ~object_name
           ~chunk_id ~fragment_id ~location =
         Lwt.ignore_result
           (Lwt_extra2.ignore_errors
              (fun () ->
                 alba_client # mgr_access # add_work_repair_fragment
                   ~namespace_id ~object_id
                   ~object_name
                   ~chunk_id
                   ~fragment_id
                   ~version_id:(snd location))) in
       Alba_client2.with_client
         albamgr_client_cfg
         ~fragment_cache
         ~manifest_cache_size
         ~bad_fragment_callback
         ~albamgr_connection_pool_size
         ~nsm_host_connection_pool_size
         ~osd_connection_pool_size
         ~osd_timeout
         ~default_osd_priority:Osd.High
         ~tls_config
         ~tcp_keepalive
         ~use_fadvise
         ~partial_osd_read
         ~cache_on_read ~cache_on_write
         ~populate_osds_info_cache:true
         ~upload_slack
         (fun alba_client ->
          Lwt.pick
            [ (alba_client # discover_osds ~check_claimed:(fun _ -> true) ());
              (alba_client # osd_access # propagate_osd_info ());
              (refresh_albamgr_cfg
                 ~loop:true
                 albamgr_client_cfg
                 alba_client
                 albamgr_cfg_url
                 ~tcp_keepalive
                 ~tls_config
              );
              (
               Networking2.make_server
                 ~max:max_client_connections
                 hosts port ~transport ~tls:None ~tcp_keepalive
                 (fun nfd ->
                  proxy_protocol alba_client albamgr_client_cfg stats nfd));
              (Lwt_extra2.make_fuse_thread ());
              Mem_stats.reporting_t ~section:Lwt_log.Section.main ();
              (let rec log_stats () =
                 Lwt_log.info_f
                   "stats:\n%s%!"
                   (ProxyStatistics.show' ~only_changed:true stats) >>= fun () ->
                 let cnt = ProxyStatistics.clear_ns_stats_changed stats in
                 Lwt_unix.sleep (max 60. (6 * cnt |> float)) >>= fun () ->
                 log_stats ()
               in
               log_stats ());
            ])
    )
    (fun exn ->
       Lwt_log.fatal_f
         ~exn
         "Going down after unexpected exception in proxy process" >>= fun () ->
       Lwt.fail exn)
