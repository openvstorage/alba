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

open Lwt
open Prelude
open Proxy_protocol

let ini_hash_to_string tbl =
  let buf = Buffer.create 20 in
  Hashtbl.iter
    (fun k v ->
       Buffer.add_string buf "[";
       Buffer.add_string buf k;
       Buffer.add_string buf "]\n";
       (Hashtbl.iter
          (fun k v ->
             Buffer.add_string buf k;
             Buffer.add_string buf " = ";
             Buffer.add_string buf v;
             Buffer.add_string buf "\n") v);
       Buffer.add_string buf "\n")
    tbl;
  Buffer.contents buf

let albamgr_cfg_to_ini_string (cluster_id, nodes) =
  let transform_node_cfg
      { Albamgr_protocol.Protocol.Arakoon_config.ips;
        port; } =
    Hashtbl.from_assoc_list
      [ ("ip", String.concat ", " ips);
        ("client_port", string_of_int port); ]
  in

  let h = Hashtbl.create 3 in
  let node_names =
    Hashtbl.fold
      (fun node_name node_cfg acc ->
         Hashtbl.add h node_name (transform_node_cfg node_cfg);
         node_name :: acc)
      nodes
      []
  in

  let global = [ ("cluster", String.concat ", " node_names);
                 ("cluster_id", cluster_id); ] in
  Hashtbl.add h "global" (Hashtbl.from_assoc_list global);

  ini_hash_to_string h

let write_albamgr_cfg albamgr_cfg destination =
  let s = albamgr_cfg_to_ini_string albamgr_cfg in
  let tmp = destination ^ ".tmp" in
  Lwt_extra2.unlink ~may_not_exist:true tmp >>= fun () ->
  Lwt_extra2.with_fd
    tmp
    ~flags:Lwt_unix.([ O_WRONLY; O_CREAT; O_EXCL; ])
    ~perm:0o664
    (fun fd ->
       Lwt_extra2.write_all
         fd
         s 0 (String.length s) >>= fun () ->
       Lwt_unix.fsync fd) >>= fun () ->
  Lwt_unix.rename tmp destination

let read_objects_slices
      (alba_client : Alba_client.alba_client)
      namespace objects_slices ~consistent_read =
  let total_length, objects_slices, n_slices, object_names =
    List.fold_left
      (fun (offset, acc, n_slices, object_names) (object_name, object_slices) ->
       let offset' =
         List.fold_left
           (fun offset (_, slice_length) -> offset + slice_length)
           offset
           object_slices
       in
       let object_names' = StringSet.add object_name object_names
       and n_slices' = n_slices + 1
       and acc' = (offset, object_name, object_slices) :: acc
       in

       offset', acc', n_slices', object_names'

      )
      (0, [], 0, StringSet.empty)
      objects_slices
  in

  let res = Bytes.create total_length in
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
    let open Cache  in
    let open Alba_statistics in
    match stat.Statistics.source with
    | Fast  -> incr fc_hits
    | Slow
    | Stale -> incr fc_misses
  in
  logger n_slices total_length n_objects >>= fun () ->
  strategy
    (fun (offset, object_name, object_slices) ->
     alba_client # download_object_slices
                 ~namespace
                 ~object_name
                 ~object_slices
                 ~consistent_read
                 ~fragment_statistics_cb
                 (fun dest_off src off len ->
                  Lwt_bytes.blit_to_bytes src off res (offset + dest_off) len;
                  Lwt.return ())
     >>= function
     | None -> Protocol.Error.failwith Protocol.Error.ObjectDoesNotExist
     | Some (_mf, mf_src) -> Lwt.return mf_src
    )
    objects_slices
  >>= fun mf_sources ->
  Lwt.return (res,n_slices,n_objects, mf_sources, !fc_hits, !fc_misses)

let render_request_args: type i o. (i,o) Protocol.request -> i -> Bytes.t =
  let open Protocol in
  function
  | ListNamespaces  -> fun { RangeQueryArgs.first; finc; last; max; reverse} ->
                       "{ }"
  | CreateNamespace -> fun (namespace, preset_option) ->
                       Printf.sprintf "(%S,_)" namespace
  | DeleteNamespace -> fun namespace ->
                       Printf.sprintf "(%S)" namespace
  | ReadObjectFs    -> fun (namespace, object_name, _,_,_ ) ->
                       Printf.sprintf "(%S,%S,_,_,_)" namespace object_name
  | WriteObjectFs   -> fun (namespace, object_name, _,_,_)  ->
                       Printf.sprintf "(%S,%S,_,_,_)" namespace object_name
  | ReadObjectsSlices -> fun (namespace, objects_slices, consistent_read) ->
                         Printf.sprintf
                           "(%S,%s )"
                           namespace
                           ([%show : (object_name *
                                        (offset * length) list) list ]
                           objects_slices)
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
  | GetClientConfig -> fun _ -> "()"

let log_request code maybe_renderer time =
  let log = if time < 0.5 then  Lwt_log.debug_f else Lwt_log.info_f in
  let details =
    match maybe_renderer with
    | None -> ""
    | Some renderer -> renderer ()
  in
  log "Request %s %s took %f" (Protocol.code_to_txt code) details time


let proxy_protocol (alba_client : Alba_client.alba_client)
                   (albamgr_client_cfg:Albamgr_protocol.Protocol.Arakoon_config.t ref)
                   (stats: ProxyStatistics.t')
                   (nfd:Net_fd.t) ic =

  let execute_request : type i o. (i, o) Protocol.request ->
                             ProxyStatistics.t' ->
                             i -> o Lwt.t
                          =
    let open Protocol in
    function
    | ListNamespaces -> fun stats { RangeQueryArgs.first; finc; last; max; reverse } ->
      (* TODO only return namespaces which are active? hmm, maybe creating too... *)
      alba_client # mgr_access # list_namespaces
        ~first ~finc ~last
        ~max ~reverse >>= fun ((cnt, namespaces), has_more) ->
      Lwt.return ((cnt, List.map fst namespaces), has_more)
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
         | Some (mf,download_stats) ->
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
    | WriteObjectFs -> fun stats (namespace,
                                  object_name,
                                  input_file,
                                  allow_overwrite,
                                  checksum_o) ->
      let open Nsm_model in
      Lwt.catch
        (fun () ->
           alba_client # upload_object_from_file
             ~namespace
             ~object_name ~input_file
             ~checksum_o
             ~allow_overwrite:(if allow_overwrite
                               then Unconditionally
                               else NoPrevious)
           >>= fun (_mf , upload_stats) ->
           let open Alba_statistics.Statistics in
           ProxyStatistics.new_upload stats namespace upload_stats.total;
           Lwt.return ()
        )
        (let open Alba_client_errors.Error in
          function
          | Err.Nsm_exn (Err.Overwrite_not_allowed, _) ->
            Protocol.Error.failwith Protocol.Error.OverwriteNotAllowed
          | Exn FileNotFound ->
             Protocol.Error.failwith Protocol.Error.FileNotFound
          | exn -> Lwt.fail exn)
    | DeleteObject -> fun stats (namespace, object_name, may_not_exist) ->
      Lwt.catch
        (fun () ->
           alba_client # delete_object ~namespace ~object_name ~may_not_exist)
        (function
          | Nsm_model.Err.Nsm_exn (Nsm_model.Err.Overwrite_not_allowed, _) ->
            Protocol.Error.failwith Protocol.Error.ObjectDoesNotExist
          | exn -> Lwt.fail exn)
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
       >>= fun (delay, (bytes, n_slices, n_objects, mf_sources, fc_hits, fc_misses )) ->
       let total_length = Bytes.length bytes in
       ProxyStatistics.new_read_objects_slices
         stats namespace
         ~total_length ~n_slices ~n_objects ~mf_sources
         ~fc_hits ~fc_misses
         ~took:delay;
       Lwt.return bytes

    | InvalidateCache ->
      fun stats namespace -> alba_client # invalidate_cache namespace
    | DropCache ->
      fun stats namespace -> alba_client # drop_cache namespace
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
                        (state:Osd_state.t)) (c,acc) ->
            let c' = c+1
            and acc' = (osd_id, osd, state) :: acc
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
  in
  let return_err_response ?msg err =
    let res_s =
      serialize_with_length
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
           let req = Deser.from_buffer (Protocol.deser_request_i r) buf in
           execute_request r stats req >>= fun res ->
           Lwt.return (serialize_with_length
                         (Llio.pair_to
                            Llio.int_to
                            (Deser.to_buffer (Protocol.deser_request_o r)))
                         (0, res),
                       let renderer () = render_request_args r req in
                       Some renderer))
      (function
        | End_of_file as e ->
          Lwt.fail e
        | Protocol.Error.Exn err ->
           let msg = Protocol.Error.show err in
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
        | Nsm_model.Err.Nsm_exn (err, _) ->
          begin
            let open Nsm_model.Err in
            match err with
            | Object_not_found ->
              return_err_response Protocol.Error.ObjectDoesNotExist
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
    Net_fd.write_all res nfd >>= fun () ->
    Lwt.return maybe_renderer

  in
  let rec inner () =
    Llio.input_string ic >>= fun req_s ->
    let buf = Llio.make_buffer req_s 0 in
    let code = Llio.int_from buf in
    with_timing_lwt
      (fun () -> handle_request buf code)
    >>= fun (time_inner, maybe_renderer) ->
    log_request code maybe_renderer time_inner >>= fun () ->
    inner ()
  in
  Llio.input_int32 ic >>= fun magic ->
  if magic = Protocol.magic
  then
    begin
      Llio.input_int32 ic >>= fun version ->
      if version = Protocol.version
      then inner ()
      else
        let err = Protocol.Error.ProtocolVersionMismatch
        and msg = Printf.sprintf
                    "protocol version: (server) %li <> %li (client)"
                    Protocol.version version
        in
        return_err_response ~msg err >>= fun (res, _) ->
        Net_fd.write_all res nfd
    end
  else Lwt.return ()



let refresh_albamgr_cfg
    ~loop
    albamgr_client_cfg
    (alba_client : Alba_client.alba_client)
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
          retrieve_cfg_from_any_node ~tls:None !albamgr_client_cfg
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
      write_albamgr_cfg ccfg destination >>= fun () ->
      Lwt_extra2.sleep_approx 60. >>= fun () ->
      if loop
      then inner ()
      else Lwt.return ()
  in
  inner ()

let run_server hosts port
               cache_dir albamgr_client_cfg
               ~manifest_cache_size
               ~fragment_cache_size
               ~albamgr_connection_pool_size
               ~nsm_host_connection_pool_size
               ~osd_connection_pool_size
               ~osd_timeout
               ~albamgr_cfg_file
               ~max_client_connections
               ~tls_config
  =
  Lwt_log.info_f "proxy_server version:%s" Alba_version.git_revision
  >>= fun () ->
  let stats = ProxyStatistics.make () in

  let rec fragment_cache_disk_usage_t () =
    Lwt.catch
      (fun () ->
       Fsutil.lwt_disk_usage cache_dir >>= fun (used_b, total_b) ->
       let percentage =
         100.0 *. ((Int64.to_float used_b) /. (Int64.to_float total_b))
       in
       Lwt_log.info_f "fragment_cache disk_usage: %.2f%%" percentage)
      (fun exn -> Lwt_log.warning ~exn "fragment_cache_disk_usage_t")
    >>= fun () ->
    Lwt_unix.sleep 60.0 >>= fun () ->
    fragment_cache_disk_usage_t ()
  in


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
       Alba_client.with_client
         albamgr_client_cfg
         ~cache_dir
         ~fragment_cache_size
         ~manifest_cache_size
         ~bad_fragment_callback
         ~albamgr_connection_pool_size
         ~nsm_host_connection_pool_size
         ~osd_connection_pool_size
         ~osd_timeout
         ~default_osd_priority:Osd.High
         ~tls_config
         (fun alba_client ->
          Lwt.pick
            [ (alba_client # discover_osds ~check_claimed:(fun _ -> true) ());
              (alba_client # osd_access # propagate_osd_info ());
              (refresh_albamgr_cfg
                 ~loop:true
                 albamgr_client_cfg
                 alba_client
                 albamgr_cfg_file
              );
              (let buffer_size = 8192 in
               let buffer_pool = Buffer_pool.create ~buffer_size in
               let ctx = None in
               Networking2.make_server
                 ~max:max_client_connections
                 hosts port ~ctx
                 (fun nfd ->
                  Buffer_pool.with_buffer
                    buffer_pool
                    (fun buffer ->
                     let ic = Net_fd.make_ic ~buffer nfd in
                     proxy_protocol alba_client albamgr_client_cfg stats nfd ic)));
              (Lwt_extra2.make_fuse_thread ());
              Mem_stats.reporting_t ~section:Lwt_log.Section.main ();
              (fragment_cache_disk_usage_t ());
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
