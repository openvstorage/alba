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

open Cmdliner
open Lwt.Infix
open! Prelude
open Cli_common

let () =
  let engine =
    try
      let s = Sys.getenv "ALBA_LWT_ENGINE" in
      Lwt_log.ign_warning_f "found env var ALBA_LWT_ENGINE=%s%!" s;
      match s with
      | "rselect" -> (new Lwt_rsocket.rselect :> Lwt_engine.t)
      | "select"  -> (new Lwt_engine.select :> Lwt_engine.t)
      | "libev"   -> (new Lwt_engine.libev () :> Lwt_engine.t)
      | str ->
         let msg =
           Printf.sprintf
             "%s: invalid ALBA_LWT_ENGINE specified, must be 'rselect', 'select' or 'libev'"
             str
         in
         Printf.eprintf "%s" msg;
         failwith msg
    with Not_found -> (new Lwt_engine.libev () :> Lwt_engine.t)
  in
  Lwt_engine.set engine;
  Lwt.async_exception_hook :=
    (fun exn -> Lwt_log.ign_info_f ~exn "Caught async exception")

let alba_get_id cfg_file tls_config to_json verbose attempts =
  let t () =
    with_albamgr_client
      cfg_file ~attempts tls_config
      (fun client ->
         client # get_alba_id >>= fun alba_id ->
         if to_json
         then begin
           let open Alba_json.AlbaId in
           print_result (make alba_id) to_yojson
         end else
           Lwt_io.printlf "The id for this alba instance is %s" alba_id)
  in
  lwt_cmd_line ~to_json ~verbose t

let alba_get_id_cmd =
  Term.(pure alba_get_id
        $ alba_cfg_url
        $ tls_config
        $ to_json $ verbose $ attempts 1),
  Term.info "get-alba-id" ~doc:"Get the unique alba instance id"



let alba_list_ns_osds cfg_file tls_config namespace to_json verbose =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
         client # nsm_host_access # with_namespace_id
           ~namespace
           (fun namespace_id ->
              client # mgr_access # list_all_namespace_osds ~namespace_id)
         >>= fun (i,osds) ->
         let open Albamgr_protocol.Protocol.Osd in
         if to_json
         then print_result osds [%to_yojson : (id * NamespaceLink.state) list]
         else Lwt_io.printlf "osds : %s\n" ([%show : (id * NamespaceLink.state) list] osds)
      )
  in
  lwt_cmd_line ~to_json ~verbose t


let alba_list_ns_osds_cmd =
  let alba_list_ns_osds_t =
    Term.(pure alba_list_ns_osds
          $ alba_cfg_url
          $ tls_config
          $ namespace 0
          $ to_json
          $ verbose)
  in
  let info =
    let doc = "list OSDs coupled to the specified namespace (alias for list-namespace-osds)" in
    Term.info "list-ns-osds" ~doc
  in
  alba_list_ns_osds_t, info

let alba_list_ns_osds_cmd2 =
  let alba_list_ns_osds_t =
    Term.(pure alba_list_ns_osds
          $ alba_cfg_url
          $ tls_config
          $ namespace 0
          $ to_json
          $ verbose)
  in
  let info =
    let doc = "list OSDs coupled to the specified namespace" in
    Term.info "list-namespace-osds" ~doc
  in
  alba_list_ns_osds_t, info


let _render_namespace name namespace stats =
  let { Nsm_model.NamespaceStats.logical_size;
        storage_size;
        storage_size_per_osd;
        bucket_count; } = stats
  in
  let open Albamgr_protocol.Protocol in
  Lwt_io.printlf "(%s,%s)" name
                 ([%show : Namespace.t] namespace)
  >>= fun () ->
  Lwt_io.printlf "logical: %Li" logical_size >>= fun () ->
  Lwt_io.printlf "storage: %Li" storage_size >>= fun () ->
  Lwt_io.printlf
    "storage_per_osd: %s"
    ([%show : (int64 * int64) list] (snd storage_size_per_osd)) >>= fun () ->
  Lwt_io.printlf
    "bucket_count: %s"
    ([%show : (Policy.policy * int64) list] (snd bucket_count))

let alba_show_namespace cfg_file tls_config namespace to_json verbose =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
         client # mgr_access # get_namespace ~namespace >>= fun ro ->
          match ro with
          | None -> Lwt.fail Not_found
          | Some (namespace, r) ->
             client # get_base_client # with_nsm_client ~namespace
               (fun nsm_client ->
                  nsm_client # get_stats >>= fun ({ Nsm_model.NamespaceStats.logical_size;
                                                   storage_size;
                                                   storage_size_per_osd;
                                                   bucket_count; } as stats) ->
                  if to_json
                  then
                    begin
                      let open Alba_json.Namespace  in
                      let res = { Statistics.logical = logical_size;
                                  storage = storage_size;
                                  storage_per_osd = snd storage_size_per_osd;
                                  bucket_count = snd bucket_count; }
                      in
                      print_result res Statistics.to_yojson
                    end
                  else
                    begin
                      _render_namespace namespace r  stats
                    end
               ))
  in
  lwt_cmd_line ~to_json ~verbose t

let alba_show_namespace_cmd =
  Term.(pure alba_show_namespace
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ to_json $ verbose
  ),
  Term.info "show-namespace" ~doc:"show information (such as total size, size per osd, and buckets) about a namespace"

let alba_show_namespaces cfg_file tls_config first finc last max reverse to_json verbose =
  let last = Option.map (fun x -> x,true) last in
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
       let list_all () =
         let needed =
           if max < 0
           then max_int
           else max
         in
         let rec inner (acc_count, acc_namespaces, acc_has_more) ~first ~finc needed =
           if needed <= 0 || not acc_has_more
           then Lwt.return (acc_count, List.rev acc_namespaces)
           else
             client # mgr_access # list_namespaces
                    ~first ~finc ~last ~max:needed ~reverse
             >>= fun ((count, namespaces), has_more) ->
             let acc_count' = count + acc_count
             and acc_namespaces' = List.rev_append namespaces acc_namespaces
             and acc_has_more' = has_more
             and needed' = needed - count
             in
             let first' =
               match List.hd acc_namespaces' with
               | None -> first
               | Some (first', _) -> first'
             and finc' = false
             in
             inner (acc_count', acc_namespaces', acc_has_more')
                   ~first:first' ~finc:finc' needed'
         in
         inner (0,[],true) ~first ~finc needed
       in
       list_all () >>= fun (count, namespaces) ->
       let namespaces_by_nsm_host =
         List.group_by
           (fun (_, namespace) ->
            namespace.Albamgr_protocol.Protocol.Namespace.nsm_host_id)
           namespaces
         |> Hashtbl.to_assoc_list
       in
       Lwt_list.map_p
         (fun (nsm_host_id, namespaces) ->
          client # get_base_client
                 # nsm_host_access
                 # with_nsm_host_client ~nsm_host_id
                 (fun client ->
                  (new Nsm_host_client.client
                       (client :> Nsm_host_client.basic_client))
                    # get_nsm_stats
                    (List.map
                       (fun (_, ns) ->
                        ns.Albamgr_protocol.Protocol.Namespace.id)
                       namespaces)) >>= function
          | Some res ->
             List.combine
               namespaces
               res
             |> List.map_filter_rev
                  (fun ((name, ns), stat) ->
                   match stat with
                   | Result.Ok stat -> Some (name, ns, stat)
                   | Result.Error _ -> None)
             |> Lwt.return
          | None ->
             Lwt_list.map_p
               (fun (name, namespace) ->
                client # get_base_client # with_nsm_client ~namespace:name
                       (fun nsm_client ->
                        nsm_client # get_stats >>= fun stats ->
                        Lwt.return (name,namespace,stats)
                       )
               )
               namespaces)
         namespaces_by_nsm_host
       >>= fun r ->
       let r = List.flatten_unordered r in
       if to_json
       then
         begin
           let transform (name,namespace,stats) =
             let { Nsm_model.NamespaceStats.logical_size;
                   storage_size;
                   storage_size_per_osd;
                   bucket_count; } = stats
             in
             let open Alba_json.Namespace  in
             let res = { Statistics.logical = logical_size;
                         storage = storage_size;
                         storage_per_osd = snd storage_size_per_osd;
                         bucket_count = snd bucket_count; }
             in
             let namespace_j = Alba_json.Namespace.make name namespace
             and statistics_j = res
             in
             {Both.name ;
              namespace = namespace_j;
              statistics = statistics_j
             }
           in
           let json = List.map transform r in
           print_result
             (count, json)
             Alba_json.Namespace.Both.c_both_list_to_yojson
         end
       else
         begin
           begin
             Lwt_io.printlf "count:%i" count >>= fun () ->
             Lwt_list.iter_s
               (fun (name,namespace, stats) ->
                _render_namespace name namespace stats
               ) r
           end
         end
      )
  in
  lwt_cmd_line ~to_json ~verbose t

let alba_show_namespaces_cmd =
  Term.(pure alba_show_namespaces
        $ alba_cfg_url
        $ tls_config
        $ first $ finc $ last $ max $ reverse
        $ to_json $ verbose
  ),
  Term.info "show-namespaces" ~doc:"show information (such as total size, size per osd, and buckets) about a range of namespaces"

let alba_upload_object
    cfg_file tls_config
    namespace
    (input_file:string)
    (object_name:string)
    (allow_overwrite : bool)
    verbose unescape
  =
  let object_name = maybe_unescape unescape object_name in
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun alba_client ->
        alba_client # upload_object_from_file
           ~epilogue_delay:None
           ~namespace
           ~object_name ~input_file
           ~allow_overwrite:(let open Nsm_model in
                             if allow_overwrite
                             then Unconditionally
                             else NoPrevious)
           ~checksum_o:None
         >>= fun _ ->
         Lwt_io.printlf "object %s was successfully uploaded"
                        input_file
      )
  in
  lwt_cmd_line ~to_json:false ~verbose t

let alba_upload_object_cmd =
  Term.(pure alba_upload_object
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ file_upload 1
        $ object_name_upload 2
        $ allow_overwrite
        $ verbose
        $ unescape
       )
  ,
  Term.info "upload-object" ~doc:"upload an object to alba"

let alba_download_object
  cfg_file tls_config
  namespace
  object_name output_file
  to_json verbose
  unescape
  =
  let object_name = maybe_unescape unescape object_name in

  let t () =
    with_alba_client
      cfg_file tls_config
      (fun alba_client ->
         alba_client # download_object_to_file
           ~namespace
           ~object_name
           ~output_file
           ~consistent_read:true ~should_cache:false
      )
    >>= fun ro ->
    let msg () =
      Printf.sprintf "no object with name %s could be found" object_name
    in
    if to_json
    then
      match ro with
      | None -> failwith (msg ())
      | Some (_,_,_) ->
         unit_result true ()
    else
      match ro with
      | None ->
         Lwt_io.printl (msg ())
      | Some (manifest, _, _) ->
         Lwt_io.printlf "object %s with size %Li downloaded to file %s"
                        object_name manifest.Nsm_model.Manifest.size output_file
  in
  lwt_cmd_line ~to_json ~verbose t

let alba_download_object_cmd =
  Term.(pure alba_download_object
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ object_name_download 1
        $ file_download 2
        $ to_json
        $ verbose
        $ unescape
  ),
  Term.info "download-object" ~doc:"download an object from alba"

let alba_delete_object
      cfg_file tls_config
      namespace object_name
      verbose unescape =
  let object_name = maybe_unescape unescape object_name in
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun alba_client ->
         alba_client # delete_object
                     ~namespace
                     ~object_name
                     ~may_not_exist:false)
  in
  lwt_cmd_line ~to_json:false ~verbose t

let alba_delete_object_cmd =
  Term.(pure alba_delete_object
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ Arg.(required &
             pos 1 (some string) None &
               info [] ~docv:"OBJECT NAME" ~doc:"the object to delete from alba")
        $ verbose
        $ unescape
  ),
  Term.info "delete-object" ~doc:"delete an object from alba"

let alba_show_object
      cfg_file tls_config
      namespace object_name
      attribute_name
      to_json verbose unescape
  =
  let object_name = maybe_unescape unescape object_name in

  let t () =
    with_alba_client
      cfg_file tls_config
      (fun alba_client ->
       let open Nsm_model in
       alba_client # get_object_manifest ~namespace
                   ~object_name
                   ~consistent_read:true ~should_cache:false
       >>= fun (hm,r) ->
       match r with
         | None ->
            Lwt.fail_with "not found"
         | Some manifest ->
            begin
              if to_json
              then
                begin
                  print_result manifest Alba_json.Manifest.to_yojson
                end
              else
                begin
                  match attribute_name with
                  | None -> Lwt_io.printlf "%s"
                                           ([%show: Manifest.t ]
                                              manifest)
                  | Some "checksum" ->
                     let open Manifest in
                     Lwt_io.printlf "%s"
                                    ([%show : Checksum.t]
                                       manifest.checksum)
                  | Some x -> Lwt_io.eprintlf "no such attribute `%s`" x
                end
            end
      )
  in
  lwt_cmd_line ~to_json ~verbose t

let alba_show_object_cmd =
  let doc =
    "print some info about the object."
    ^ " This call serves development purposes, and the output format"
    ^ " (even the json format) can change without notice."
  in
  Term.(pure alba_show_object
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ Arg.(required &
                 pos 1 (some string) None &
                 info [] ~docv:"OBJECT NAME" ~doc:"name of the object")
        $ Arg.(value & opt (some string) None &
                 info ["attribute"] ~docv:"ATTRIBUTE"
                      ~doc:"nothing or 'checksum' ")
        $ to_json $ verbose
        $ unescape
  ),
  Term.info "show-object" ~doc

let alba_list_objects cfg_file tls_config namespace verbose =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun alba_client ->
         alba_client # get_base_client # with_nsm_client
           ~namespace
           (fun nsm ->
              nsm # list_all_objects ())) >>= fun (cnt, objs) ->
    Lwt_io.printlf
      "Found %i objects: %s"
      cnt
      ([%show: string list] objs)
  in
  lwt_cmd_line ~to_json:false ~verbose t

let alba_list_objects_cmd =
  Term.(pure alba_list_objects
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ verbose
  ),
  Term.info "list-objects" ~doc:"list all objects in the specified namespace"

let alba_get_nsm_version cfg_file tls_config nsm_host_id verbose =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun alba_client ->
       let nsm = alba_client # nsm_host_access # get ~nsm_host_id in
       nsm # get_version >>= fun (major,minor,patch, hash) ->
       Lwt_io.printlf "(%i, %i, %i, %S)"
         major minor patch hash
      )
  in
  lwt_cmd_line ~to_json:false ~verbose t

let alba_get_nsm_version_cmd =
  Term.(pure alba_get_nsm_version
        $ alba_cfg_url
        $ tls_config
        $ nsm_host 0
        $ verbose
  ),
  Term.info "nsm-get-version" ~doc:"nsm host's version info"



let alba_create_namespace cfg_file tls_config namespace preset_name verbose =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
         client # create_namespace ~preset_name ~namespace () >>= fun namespace_id ->
         Lwt_io.printlf "Create namespace successful, got id %Li" namespace_id
      )
  in
  lwt_cmd_line ~to_json:false ~verbose t

let alba_create_namespace_cmd =
  Term.(pure alba_create_namespace
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ preset_name_namespace_creation 1
        $ verbose
  ),
  Term.info "create-namespace" ~doc:"create a namespace with the given name"

let alba_delete_namespace cfg_file tls_config namespace verbose =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
         client # delete_namespace ~namespace >>= fun () ->
         Lwt_io.printlf "Delete namespace successful"
      )
  in
  lwt_cmd_line ~to_json:false ~verbose t

let alba_delete_namespace_cmd =
  Term.(pure alba_delete_namespace
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ verbose
  ),
  Term.info "delete-namespace" ~doc:"delete the namespace with the given name"


let alba_get_disk_safety
      cfg_file
      tls_config
      long_ids node_ids namespaces
      include_decommissioning_as_dead
      include_errored_as_dead
      to_json verbose
  =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
         (if namespaces = []
          then begin
            client # mgr_access # list_all_namespaces >>= fun (_, namespaces) ->
            Lwt.return namespaces
          end else begin
            (* TODO the albamgr should support getting this with 1 call *)
            Lwt_list.map_p
              (fun namespace ->
                 client # mgr_access # get_namespace ~namespace >>= function
                 | None -> Lwt.fail_with (Printf.sprintf "unknown namespace %s" namespace)
                 | Some ns_info -> Lwt.return ns_info)
              namespaces
          end) >>= fun namespaces ->

         client # mgr_access # get_maintenance_config >>= fun maintenance_config ->
         let now = Unix.gettimeofday () in
         let t_should_have_activity = now -. maintenance_config.Maintenance_config.auto_repair_timeout_seconds in

         client # mgr_access # list_all_claimed_osds >>= fun (_, all_osds) ->
         let dead_osds =
           List.filter
             (fun (osd_id, osd_info) ->
               let open Nsm_model.OsdInfo in
               (include_decommissioning_as_dead && osd_info.decommissioned)
               || (include_errored_as_dead &&
                     (match osd_info.read, osd_info.write, osd_info.errors with
                      | t_read :: _, t_write :: _, (t_error, _) :: _ ->
                         (* errors are more recent than reads/writes *)
                         (Pervasives.max t_error t_should_have_activity) > (min t_read t_write)
                      | t_read :: _, t_write :: _, [] ->
                        t_should_have_activity > (min t_read t_write)
                      | _, [], _
                      | [], _, _ ->
                         begin
                           match osd_info.claimed_since with
                           | None -> true
                           | Some tc -> tc +. 60.0 <= now
                         end
                     )
                  )
               || List.mem (get_long_id osd_info.kind) long_ids
               || List.mem osd_info.node_id node_ids)
             all_osds
         in

         let actual_long_ids =
           List.map
             (fun (_, osd_info) -> Nsm_model.OsdInfo.(get_long_id osd_info.kind))
             dead_osds
         in
         List.iter
           (fun long_id ->
             if not (List.mem long_id actual_long_ids)
             then failwith (Printf.sprintf "unknown osd %s" long_id))
           long_ids;

         let dead_osds = List.map fst dead_osds in

         (client # mgr_access # list_all_purging_osds >>= fun (_, osds') ->
          Lwt.return (List.rev_append
                        dead_osds
                        osds'))
         >>= fun dead_osds ->

         Disk_safety.get_disk_safety client namespaces dead_osds >>= fun res ->

         if to_json
         then
           begin
             let open Alba_json.DiskSafety in
             print_result
               (List.map
                  (fun (namespace, bucket_safety) ->
                    let open Disk_safety in
                    let safety, safety_count =
                      List.minima
                        ~compare:(fun s1 s2 -> compare s1.remaining_safety s2.remaining_safety)
                        bucket_safety
                      |> function
                        | [] -> None, None
                        | { remaining_safety; count } :: tl ->
                           Some remaining_safety,
                           Some (List.fold_left
                                   (fun safety_count { count; _ } -> Int64.add safety_count count)
                                   count
                                   tl)
                    in
                    { namespace;
                      safety;
                      safety_count;
                      bucket_safety;
                  })
                  res)
               t_list_to_yojson
           end
         else
           begin
             Lwt_list.iter_s
               (fun (namespace, safety) ->
                  match List.hd safety with
                  | None ->
                    Lwt_log.info_f "No objects in namespace %s, so infinite safety ;)" namespace
                  | Some { Disk_safety.bucket; count; applicable_dead_osds; remaining_safety; } ->
                    Lwt_log.info_f
                      "%Li objects in namespace %s with bucket %s have safety %i remaining"
                      count namespace (Policy.show_policy bucket) remaining_safety)
               res
           end
      )
  in
  lwt_cmd_line ~to_json ~verbose t

let alba_get_disk_safety_cmd =
  let long_ids =
    let doc = "$(docv) of the dead/unreachable osds" in
    Arg.(value
         & opt_all string []
         & info ["long-id"] ~docv:"LONG_ID" ~doc)
  in
  let node_ids =
    Arg.(value
         & opt_all string []
         & info [ "node-id" ] ~docv:"NODE_ID" ~doc:"$(docv) of the dead/unreachable nodes")
  in
  let namespaces =
    Arg.(value
         & opt_all string []
         & info
           ["namespace"]
           ~docv:"NAMESPACE"
           ~doc:"namespaces for which the disk safety should be checked (specify none for all namespaces)")
  in
  let include_decommissioning_as_dead =
    Arg.(value
         & flag
         & info
           ["include-decommissioning-as-dead"]
           ~doc:"assume all previously decommissioned disks are no longer available")
  in
  let include_errored_as_dead =
    Arg.(value
         & flag
         & info
             [ "include-errored-as-dead" ]
             ~doc:"include osds with errors as no longer available")
  in
  Term.(pure alba_get_disk_safety
        $ alba_cfg_url
        $ tls_config
        $ long_ids
        $ node_ids
        $ namespaces
        $ include_decommissioning_as_dead
        $ include_errored_as_dead
        $ to_json $ verbose
  ),
  Term.info "get-disk-safety" ~doc:"simulate the available disk safety (how many disks can we still lose before any data is lost) for the given namespaces, assuming the specified osds are lost"

let namespace_recovery_agent
      cfg_file tls_config
      namespace
      home
      workers worker_id
      osds verbose
  =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
         Recover_nsm_host.nsm_recovery_agent
           client namespace home
           workers worker_id
           osds)
  in
  lwt_cmd_line ~to_json:false ~verbose t

let namespace_recovery_agent_cmd =
  Term.(pure namespace_recovery_agent
        $ alba_cfg_url
        $ tls_config
        $ namespace 0
        $ Arg.(required &
               pos 1 (some dir) None &
               info [] ~docv:"HOME" ~doc:"home dir for the recovery agent")
        $ Arg.(required &
               pos 2 (some int) None &
               info [] ~docv:"MODULO" ~doc:"total number of workers for this namespace")
        $ Arg.(required &
               pos 3 (some int) None &
               info [] ~docv:"REMAINDER" ~doc:"number of the worker")
        $ Arg.(value
               & opt_all int64 []
               & info ["osd-id"]
                      ~docv:"OSD_ID"
                      ~doc:"osd to wait for")
        $ verbose
  ),
  Term.info "namespace-recovery-agent" ~doc:"recover the contents of a namespace from the osds"

let unit_tests produce_xml alba_cfg_url tls_config only_test =
  Albamgr_test.ccfg_url_ref := Some alba_cfg_url;
  let () = Albamgr_test._tls_config_ref := tls_config in
  let () =
    Lwt_main.run
      (install_logger ~log_sinks:`Stdout ~subcomponent:"unit-tests" ~verbose:true ())
  in

  let rec rc_of =
    let open OUnit in
    function
    | [] -> 0
    | RSuccess _::t
    | RSkip _::t -> rc_of t
    | RFailure _::_
    | RError _::_
    | RTodo _::_ ->
       1
  in
  let _my_run only_test suite produce_xml =
    let open OUnit in
    let nsuite =
      if only_test = []
      then suite
      else
        begin
          match test_filter ~skip:true only_test suite with
          | Some test -> test
          | None ->
             failwith ("Filtering test "^
                         (String.concat ", " only_test) ^
                           " lead to no test")
        end
    in
    if produce_xml
    then OUnit_XML.run_suite_producing_xml nsuite "testresults.xml"
    else run_test_tt ~verbose:true nsuite


  in
  let suite = Test.suite in
  Printf.printf "%s\n" ([%show: string array] Sys.argv);
  let results = _my_run only_test suite produce_xml in
  let () = Gc.full_major() in
  let rc = rc_of results in
  exit rc


let unit_tests_cmd =

  Term.(pure unit_tests
        $ produce_xml false
        $ alba_cfg_url
        $ tls_config
        $ only_test
  ),
  Term.info "unit-tests" ~doc:"run unit tests"


let () =
  let () = Sys.set_signal Sys.sigpipe Sys.Signal_ignore in
  let print_version terse =
    let open Alba_version in
    let () =
      match modifier with
      | None   -> Printf.printf "%i.%i.%i\n" major minor patch
      | Some s -> Printf.printf "%i.%i.%i-%s\n" major minor patch s
    in
    if not terse
    then
      begin
        Printf.printf "git_revision: %S\n" git_revision;
        Printf.printf "git_repo: %S\n" git_repo;
        Printf.printf "compile_time: %S\n" compile_time;
        Printf.printf "machine: %S\n" machine;
        Printf.printf "model_name: %S\n" model_name;
        Printf.printf "compiler_version: %S\n" compiler_version;
        Printf.printf "is_alba_test: %b\n" is_alba_test;
        Printf.printf "dependencies:\n%s\n" dependencies

      end
  in
  let help_cmd =
    let unit_arg = Arg.(value &
                        flag &
                        info ["terse"]
                          ~doc:"Keep it terse and only specify the version") in
    Term.(pure print_version $ unit_arg),
    Term.info "version" ~doc:"Show version information"
  in
  let copts_sect = "COMMON OPTIONS" in
  let copts_t = Term.pure "x" in
  let help_secs = [
      `S copts_sect;
      `P "These options are common to all commands.";
      `S "MORE HELP";
      `P "Use `$(mname) $(i,COMMAND) --help' for help on a single command.";`Noblank;
      `P "Use `$(mname) help patterns' for help on patch matching."; `Noblank;
      `P "Use `$(mname) help environment' for help on environment variables.";
      `S "BUGS"; `P "Check bug reports at https://github.com/openvstorage/alba/issues";]
  in
  let default_cmd =
    let doc = "Alba Storage System" in
    let man = help_secs in
    Term.(ret (pure (fun _ -> `Help (`Pager, None)) $ copts_t)),
    Term.info "alba" ~sdocs:copts_sect ~doc ~man
  in
  let cmds1 =
    [
      help_cmd;


      alba_get_id_cmd;

      alba_get_nsm_version_cmd;

      alba_create_namespace_cmd;
      alba_delete_namespace_cmd;
      alba_show_namespace_cmd;
      alba_show_namespaces_cmd;
      alba_list_ns_osds_cmd;
      alba_list_ns_osds_cmd2;

      alba_upload_object_cmd;
      alba_list_objects_cmd;
      alba_download_object_cmd;
      alba_show_object_cmd;
      alba_delete_object_cmd;

      alba_get_disk_safety_cmd;

      namespace_recovery_agent_cmd;

      unit_tests_cmd;

    ] in
  let cmds =
    List.flatten_unordered
      [ Cli_preset.cmds;
        Cli_proxy.cmds;
        Cli_osd.cmds;
        Cli_asd.cmds;
        Cli_maintenance.cmds;
        Cli_mgr.cmds;
        Cli_messages.cmds;
        Cli_nsm_host.cmds;
        Cli_bench.cmds;
        Asd_bench.cmds;
        cmds1; ]
  in
  let cmd_names =
    "commands"
    :: List.map
         (fun (_,info) -> Term.name info )
         cmds
  in
  let names () = Printf.printf "%s\n" (String.concat " " cmd_names) in
  let names_cmd =
    Term.(pure names $ const ()),
    Term.info "commands" ~doc:"all commands"
  in
  let cmds' = names_cmd::cmds in
  match Term.eval_choice default_cmd cmds' with
  | `Error _ -> exit 1
  | _ -> exit 0
