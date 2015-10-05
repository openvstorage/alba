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

open Cmdliner
open Lwt
open Prelude
open Slice
open Cli_common

let () =
  Lwt_engine.set (new Lwt_engine.select);
  Lwt.async_exception_hook :=
    (fun exn -> Lwt_log.ign_info_f ~exn "Caught async exception")

let osd_id =
  let doc = "$(docv) of the osd to connect with" in
  Arg.(required
       & opt (some int32) None
       & info ["osd-id"] ~docv:"OSD_ID" ~doc)

let unescape =
  Arg.(value
       & flag
       & info ["unescape"] ~docv:"unescape keys")

let alba_get_id cfg_file to_json attempts =
  let t () =
    with_albamgr_client
      cfg_file ~attempts
      (fun client ->
         client # get_alba_id >>= fun alba_id ->
         if to_json
         then begin
           let open Alba_json.AlbaId in
           print_result (make alba_id) to_yojson
         end else
           Lwt_io.printlf "The id for this alba instance is %s" alba_id)
  in
  lwt_cmd_line to_json t

let alba_get_id_cmd =
  Term.(pure alba_get_id $ alba_cfg_file $ to_json $ attempts 1),
  Term.info "get-alba-id" ~doc:"Get the unique alba instance id"

let osd_multi_get osd_id cfg_file keys unescape =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         client # with_osd ~osd_id
           (fun osd ->
              Lwt_list.map_s
                (fun key ->
                   osd # get_option
                     Osd.High
                     (Slice.wrap_string (if unescape
                      then Scanf.unescaped key
                      else key)))
                keys >>= fun values_s ->
              let values = List.map(fun v -> Option.map
                                               Slice.get_string_unsafe v)
                                   values_s in
              Lwt_io.printlf "%s" ([%show : string option list] values)))
  in
  lwt_cmd_line false t

let osd_multi_get_cmd =
  let doc = "$(docv)" in
  let keys = Arg.(non_empty &
                 pos_all string [] &
                 info [] ~docv:"KEYS" ~doc) in
  Term.(pure osd_multi_get $ osd_id $ alba_cfg_file $ keys $ unescape),
  Term.info "osd-multi-get" ~doc

let osd_range osd_id cfg_file =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
       let first = Slice.wrap_string "" in
         client # with_osd ~osd_id
           (fun osd ->
              osd # range
                Osd.High
                ~first ~finc:true
                ~last:None
                ~reverse:false ~max:(-1) >>= fun ((cnt, keys_s), has_more) ->
              let keys = List.map Slice.get_string_unsafe keys_s in
              Lwt_io.printlf "%s"
                ([%show : string list] keys)))
  in
  lwt_cmd_line false t

let osd_range_cmd =
  Term.(pure osd_range $ osd_id $ alba_cfg_file),
  Term.info "osd-range" ~doc:"range query on an OSD"


let alba_claim_osd alba_cfg_file long_id to_json =
  let t () =
    with_alba_client
      alba_cfg_file
      (fun alba_client ->
         alba_client # claim_osd ~long_id) >>= fun osd_id ->
    Lwt_log.debug_f "Claimed %S with id=%li" long_id osd_id
  in
  lwt_cmd_line_unit to_json t

let alba_claim_osd_cmd =
  Term.(pure alba_claim_osd
        $ alba_cfg_file
        $ Arg.(required
               & opt (some string) None
               & info ["long-id"] ~docv:"LONG_ID")
        $ to_json),
  Term.info
    "claim-osd"
    ~doc:"claim the osd by this alba instance"

let alba_decommission_osd alba_cfg_file long_id to_json =
  let t () =
    with_alba_client
      alba_cfg_file
      (fun alba_client -> alba_client # decommission_osd ~long_id)
  in
  lwt_cmd_line_unit to_json t

let alba_decommission_osd_cmd =
  Term.(pure alba_decommission_osd
        $ alba_cfg_file
        $ Arg.(required
               & opt (some string) None
               & info ["long-id"] ~docv:"LONG_ID"
          )
        $ to_json
  ),
  Term.info
    "decommission-osd"
    ~doc:"tell this alba instance we are no longer going to use the osd"

let alba_list_ns_osds cfg_file namespace =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         client # nsm_host_access # with_namespace_id
           ~namespace
           (fun namespace_id ->
              client # mgr_access # list_all_namespace_osds ~namespace_id)
         >>= fun (i,osds) ->
         let open Albamgr_protocol.Protocol.Osd in
         Lwt_io.printlf "osds : %s\n" ([%show : (id * NamespaceLink.state) list] osds)
         >>= fun () ->
         Lwt.return ())
  in
  lwt_cmd_line false t


let alba_list_ns_osds_cmd =
  let alba_list_ns_osds_t =
    Term.(pure alba_list_ns_osds
          $ alba_cfg_file
          $ namespace 0) in
  let info =
    let doc = "list OSDs coupled to the specified namespace" in
    Term.info "list-ns-osds" ~doc
  in
  alba_list_ns_osds_t, info

let alba_show_namespace cfg_file namespace to_json =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         client # mgr_access # get_namespace ~namespace >>= fun ro ->
          match ro with
          | None -> Lwt_io.printlf "not found"
          | Some (namespace, r) ->
             client # get_base_client # with_nsm_client ~namespace
               (fun nsm_client ->
                  nsm_client # get_stats >>= fun { Nsm_model.NamespaceStats.logical_size;
                                                   storage_size;
                                                   storage_size_per_osd;
                                                   bucket_count; } ->
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
                      let open Albamgr_protocol.Protocol in
                      Lwt_io.printlf "(%s,%s)" namespace
                                     ([%show : Namespace.t] r)
                      >>= fun () ->
                      Lwt_io.printlf "logical: %Li" logical_size >>= fun () ->
                      Lwt_io.printlf "storage: %Li" storage_size >>= fun () ->
                      Lwt_io.printlf
                        "storage_per_osd: %s"
                        ([%show : (int32 * int64) list] (snd storage_size_per_osd)) >>= fun () ->
                      Lwt_io.printlf
                        "bucket_count: %s"
                        ([%show : (Policy.policy * int64) list] (snd bucket_count))
                    end
               ))
  in
  lwt_cmd_line to_json t

let alba_show_namespace_cmd =
  Term.(pure alba_show_namespace
        $ alba_cfg_file
        $ namespace 0
        $ to_json
  ),
  Term.info "show-namespace" ~doc:"show information about a namespace"


let alba_upload_object
    cfg_file
    namespace
    (input_file:string)
    (object_name:string)
    (allow_overwrite : bool)
  =
  let t () =
    with_alba_client
      cfg_file
      (fun alba_client ->
         alba_client # upload_object_from_file
           ~namespace
           ~object_name ~input_file
           ~allow_overwrite:(let open Nsm_model in
                             if allow_overwrite
                             then Unconditionally
                             else NoPrevious)
           ~checksum_o:None
         >>= fun _ ->
         Lwt_io.printlf "object %s was succesfully uploaded"
                        input_file
      )
  in
  lwt_cmd_line false t

let alba_upload_object_cmd =
  Term.(pure alba_upload_object
        $ alba_cfg_file
        $ namespace 0
        $ file_upload 1
        $ object_name_upload 2
        $ allow_overwrite
       )
  ,
  Term.info "upload-object" ~doc:"upload an object to alba"

let alba_download_object
  cfg_file
  namespace
  object_name output_file =
  let t () =
    with_alba_client
      cfg_file
      (fun alba_client ->
         alba_client # download_object_to_file
           ~namespace
           ~object_name
           ~output_file
           ~consistent_read:true ~should_cache:false
      )
    >>= function
    | None ->
      Lwt_io.printlf "no object with name %s could be found" object_name
    | Some (manifest,_) ->
      Lwt_io.printlf "object %s with size %Li downloaded to file %s"
                     object_name manifest.Nsm_model.Manifest.size output_file
  in
  lwt_cmd_line false t

let alba_download_object_cmd =
  Term.(pure alba_download_object
        $ alba_cfg_file
        $ namespace 0
        $ object_name_download 1
        $ file_download 2
  ),
  Term.info "download-object" ~doc:"download an object from alba"

let alba_delete_object cfg_file namespace object_name =
  let t () =
    with_alba_client
      cfg_file
      (fun alba_client ->
         alba_client # delete_object
                     ~namespace
                     ~object_name
                     ~may_not_exist:false)
  in
  lwt_cmd_line false t

let alba_show_object
      cfg_file
      namespace object_name
      attribute_name
  =

  let t () =
    with_alba_client
      cfg_file
      (fun alba_client ->
       let open Nsm_model in
       alba_client # get_object_manifest ~namespace
                   ~object_name
                   ~consistent_read:true ~should_cache:false
       >>= fun (hm,r) ->
       match r with
         | None ->
            Lwt_io.eprintlf "not found"
         | Some manifest ->
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
      )
  in
  lwt_cmd_line false t

let alba_show_object_cmd =
  Term.(pure alba_show_object
        $ alba_cfg_file
        $ namespace 0
        $ Arg.(required &
                 pos 1 (some string) None &
                 info [] ~docv:"OBJECT NAME" ~doc:"name of the object")
        $ Arg.(value & opt (some string) None &
                 info ["attribute"] ~docv:"ATTRIBUTE"
                      ~doc:"nothing or 'checksum' ")
  ),
  Term.info "show-object" ~doc:"print some info about the object"

let alba_delete_object_cmd =
  Term.(pure alba_delete_object $
        alba_cfg_file $
        namespace 0 $
        Arg.(required &
             pos 1 (some string) None &
             info [] ~docv:"OBJECT NAME" ~doc:"the object to delete from alba")),
  Term.info "delete-object" ~doc:"delete an object from alba"

let alba_list_objects cfg_file namespace =
  let t () =
    with_alba_client
      cfg_file
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
  lwt_cmd_line false t

let alba_list_objects_cmd =
  Term.(pure alba_list_objects $
        alba_cfg_file $
        namespace 0),
  Term.info "list-objects" ~doc:"list all objects in the specified namespace"

let alba_get_nsm_version cfg_file nsm_host_id =
  let t () =
    with_alba_client
      cfg_file
      (fun alba_client ->
       let nsm = alba_client # nsm_host_access # get ~nsm_host_id in
       nsm # get_version >>= fun (major,minor,patch, hash) ->
       Lwt_io.printlf "(%i, %i, %i, %S)"
         major minor patch hash
      )
  in
  lwt_cmd_line false t

let alba_get_nsm_version_cmd =
  Term.(pure alba_get_nsm_version
        $ alba_cfg_file
        $ nsm_host 0
  ),
  Term.info "nsm-get-version" ~doc:"nsm host's version info"


let alba_list_namespaces cfg_file to_json =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         client # mgr_access # list_all_namespaces >>= fun (cnt, namespaces) ->
         if to_json
         then begin
           let res =
             List.map
               (fun (name, info) -> Alba_json.Namespace.make name info)
               namespaces
           in
           print_result res Alba_json.Namespace.t_list_to_yojson
         end else
           Lwt_io.printlf
             "Found the following namespaces: %s"
             ([%show: (string * Albamgr_protocol.Protocol.Namespace.t) list]
                namespaces)
      )
  in
  lwt_cmd_line to_json t


let alba_list_namespaces_cmd =
  Term.(pure alba_list_namespaces $ alba_cfg_file $ to_json),
  Term.info "list-namespaces" ~doc:"list all namespaces"



let alba_create_namespace cfg_file namespace preset_name =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         client # create_namespace ~preset_name ~namespace () >>= fun namespace_id ->
         Lwt_io.printlf "Create namespace successful, got id %li" namespace_id
      )
  in
  lwt_cmd_line false t

let alba_create_namespace_cmd =
  Term.(pure alba_create_namespace
        $ alba_cfg_file
        $ namespace 0
        $ preset_name_namespace_creation 1),
  Term.info "create-namespace" ~doc:"create a namespace with the given name"

let alba_delete_namespace cfg_file namespace =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         client # delete_namespace ~namespace >>= fun () ->
         Lwt_io.printlf "Delete namespace successful"
      )
  in
  lwt_cmd_line false t

let alba_delete_namespace_cmd =
  Term.(pure alba_delete_namespace
        $ alba_cfg_file
        $ namespace 0),
  Term.info "delete-namespace" ~doc:"delete the namespace with the given name"


let alba_get_disk_safety
    cfg_file
    long_ids namespaces
    include_decommissioning_as_dead
    to_json =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         let open Albamgr_protocol.Protocol in

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


         (* TODO the albamgr should support getting this with 1 call *)
         Lwt_list.map_p
           (fun long_id ->
              client # mgr_access # get_osd_by_long_id ~long_id >>= function
              | None -> Lwt.fail_with (Printf.sprintf "unknown osd %s" long_id)
              | Some (claim_info, _) ->
                begin
                  let open Osd.ClaimInfo in
                  match claim_info with
                  | ThisAlba osd_id -> Lwt.return osd_id
                  | _ ->
                    Lwt.fail_with
                      (Printf.sprintf
                         "osd %s is not claimed by this alba instance"
                         long_id)
                end)
           long_ids >>= fun osds1 ->

         (if include_decommissioning_as_dead
          then
            client # mgr_access # list_all_decommissioning_osds >>= fun (_, osds2) ->
            Lwt.return
              (List.rev_append
                 osds1
                 (List.map fst osds2))
          else
            Lwt.return osds1) >>= fun osds ->

         Disk_safety.get_disk_safety client namespaces osds >>= fun res ->

         if to_json
         then
           begin
             let open Alba_json.DiskSafety in
             print_result
               (List.map
                  (fun (namespace, safety_o) ->
                     { namespace;
                       safety = Option.map (fun (_, _, _, s) -> s) safety_o;
                     })
                  res)
               t_list_to_yojson
           end
         else
           begin
             Lwt_list.iter_s
               (fun (namespace, safety_o) ->
                  match safety_o with
                  | None ->
                    Lwt_log.info_f "No objects in namespace %s, so infinite safety ;)" namespace
                  | Some (policy, cnt, applicable_dead_osds, safety) ->
                    Lwt_log.info_f
                      "%Li objects in namespace %s with policy %s have safety %i remaining"
                      cnt namespace (Policy.show_policy policy) safety)
               res
           end
      )
  in
  lwt_cmd_line to_json t

let alba_get_disk_safety_cmd =
  let long_ids =
    let doc = "$(docv) of the dead/unreachable osds" in
    Arg.(value
         & opt_all string []
         & info ["long-id"] ~docv:"LONG_ID" ~doc)
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
  Term.(pure alba_get_disk_safety
        $ alba_cfg_file
        $ long_ids
        $ namespaces
        $ include_decommissioning_as_dead
        $ to_json),
  Term.info "get-disk-safety" ~doc:"get the disk safety"

let namespace_recovery_agent
      cfg_file
      namespace
      home
      workers worker_id
      osds
  =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         Recover_nsm_host.nsm_recovery_agent
           client namespace home
           workers worker_id
           osds)
  in
  lwt_cmd_line false t

let namespace_recovery_agent_cmd =
  Term.(pure namespace_recovery_agent
        $ alba_cfg_file
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
               & opt_all int32 []
               & info ["osd-id"]
                      ~docv:"OSD_ID"
                      ~doc:"osd to wait for")
  ),
  Term.info "namespace-recovery-agent" ~doc:"recover the contents of a namespace from the osds"

let unit_tests produce_xml alba_cfg_file only_test =
  Albamgr_test.ccfg_ref :=
    Some (Albamgr_protocol.Protocol.Arakoon_config.from_config_file alba_cfg_file);
  let () = install_logger ~channel:Lwt_io.stdout () in

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
  let _my_run only_test suite =
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
    run_test_tt ~verbose:true nsuite
  in
  let suite = Test.suite in
  Printf.printf "%s\n" ([%show: string array] Sys.argv);
  let results =
    if produce_xml
    then OUnit_XML.run_suite_producing_xml suite "testresults.xml"
    else _my_run only_test suite
  in
  let rc = rc_of results in
  exit rc


let unit_tests_cmd =
  let produce_xml default =
    let doc = "produce xml in ./testresults.xml. $(docv): bool" in
    Arg.(value
         & opt bool default
         & info ["xml"] ~docv:"XML" ~doc)
  in
  let only_test =
    Arg.(value
         & opt_all string []
         & info ["only-test"] ~docv:"ONLY-TEST" ~doc:"limit tests to filter:$(docv)"
    )
  in
  Term.(pure unit_tests
        $ produce_xml false
        $ alba_cfg_file
        $ only_test
  ),
  Term.info "unit-tests" ~doc:"run unit tests"


let () =
  let print_version terse =
    let open Alba_version in
    Printf.printf "%i.%i.%i\n" major minor patch;
    if not terse
    then
      begin
        Printf.printf "git_revision: %S\n" git_revision;
        Printf.printf "git_repo: %S\n" git_repo;
        Printf.printf "compile_time: %S\n" compile_time;
        Printf.printf "machine: %S\n" machine;
        Printf.printf "compiler_version: %S\n" compiler_version;
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
      `S "BUGS"; `P "Check bug reports at http://bugs.example.org.";]
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

      osd_multi_get_cmd;
      osd_range_cmd;

      alba_get_id_cmd;

      alba_get_nsm_version_cmd;
      alba_claim_osd_cmd;
      alba_decommission_osd_cmd;

      alba_list_namespaces_cmd;
      alba_create_namespace_cmd;
      alba_delete_namespace_cmd;
      alba_show_namespace_cmd;

      alba_list_ns_osds_cmd;

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
        Cli_asd.cmds;
        Cli_maintenance.cmds;
        Cli_mgr.cmds;
        Cli_messages.cmds;
        Cli_nsm_host.cmds;
        cmds1; ]
  in
  match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1
  | _ -> exit 0
