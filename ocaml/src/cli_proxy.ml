(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open Lwt.Infix
open Cmdliner
open Cli_common
open! Prelude

module Config = struct
  type t = {
    ips : (string list [@default []]);
    transport : (string [@default "tcp"]);
    port : int;
    log_level : string;
    albamgr_cfg_file : string option [@default None];
    albamgr_cfg_url : string option [@default None];
    fragment_cache : Fragment_cache_config.fragment_cache option [@default None];
    fragment_cache_dir : string option [@default None]; (* obsolete *)
    fragment_cache_size : int option [@default None];   (* obsolete *)
    manifest_cache_size : (int [@default 100_000]);
    albamgr_connection_pool_size : (int [@default 10]);
    nsm_host_connection_pool_size : (int [@default 10]);
    osd_connection_pool_size : (int [@default 10]);
    osd_timeout : float [@default 10.];
    lwt_preemptive_thread_pool_min_size : (int [@default 6]);
    lwt_preemptive_thread_pool_max_size : (int [@default 8]);
    chattiness : float option [@default None];
    max_client_connections : int [@default 128];
    tls_client : Tls.t option [@default None];
    tcp_keepalive : (Tcp_keepalive2.t [@default Tcp_keepalive2.default]);
    use_fadvise: bool [@default true];
    upload_slack : float [@default 0.2];
    read_preference : string list [@default []];
  } [@@deriving yojson, show]
end



let retrieve_cfg_from_string txt =
  Lwt_log.ign_info_f "Found the following config: %s" txt ;
  let config = Config.of_yojson (Yojson.Safe.from_string txt) in
  let () = match config with
   | Result.Error err ->
      Lwt_log.ign_warning_f "Error while parsing cfg file: %s" err
   | Result.Ok cfg ->
      Lwt_log.ign_info_f
        "Interpreted the config as: %s"
        ([%show : Config.t] cfg)
  in
  config

let retrieve_cfg cfg_url =
  Arakoon_config_url.retrieve cfg_url >|= retrieve_cfg_from_string


let proxy_start (cfg_url:Url.t) log_sinks =
  let t () =
    retrieve_cfg cfg_url >>= function
    | Result.Error err -> failwith err
    | Result.Ok cfg ->
       let url_from_cfg cfg =
         let open Config in
         let abm_cfg_url = Prelude.Option.map Url.make cfg.albamgr_cfg_url in
         match cfg.albamgr_cfg_file, abm_cfg_url with
         | None,Some u -> u
         | Some f,None   ->
            Lwt_log.ign_warning_f "albamgr_cfg_file is deprecated, please use albamgr_cfg_url";
            (Url.make f)
         | Some _,Some u ->
            failwith "both albamgr_file and albamgr_cfg_url were configured; just use albamgr_cfg_url"
         | None,None     ->
            failwith "neither albamgr_cfg_file nor albamgr_cfg_url configured"

       in
       let open Config in
       let albamgr_cfg_url = url_from_cfg cfg in
       let ips  = cfg.ips
       and port = cfg.port
       and transport =
         match cfg.transport with
         | "tcp" -> Net_fd.TCP
         | "rdma" -> Net_fd.RDMA
         | _ -> failwith "transport should be 'tcp' or 'rdma'"
       and log_level = cfg.log_level
       and
         manifest_cache_size,
         albamgr_connection_pool_size,
         nsm_host_connection_pool_size,
         osd_connection_pool_size, osd_timeout,
         lwt_preemptive_thread_pool_min_size, lwt_preemptive_thread_pool_max_size,
         max_client_connections, tcp_keepalive,
         use_fadvise, upload_slack,
         read_preference
         =
         cfg.manifest_cache_size,
         cfg.albamgr_connection_pool_size,
         cfg.nsm_host_connection_pool_size,
         cfg.osd_connection_pool_size, cfg.osd_timeout,
         cfg.lwt_preemptive_thread_pool_min_size, cfg.lwt_preemptive_thread_pool_max_size,
         cfg.max_client_connections, cfg.tcp_keepalive,
         cfg.use_fadvise, cfg.upload_slack,
         cfg.read_preference
       and fragment_cache_cfg =
         match cfg.fragment_cache, cfg.fragment_cache_dir, cfg.fragment_cache_size with
         | Some f, None, None ->
            f
         | Some _, Some _, None
         | Some _, None  , Some _
         | Some _, Some _, Some _
         | None  , None  , Some _ ->
            failwith "Invalid combination of fragment_cache, fragment_cache_dir & fragment_cache_size was specified"
         | None, None, None ->
            Fragment_cache_config.None'
         | None, Some path, o_fragment_cache_size ->
            Fragment_cache_config.(
             Local { path;
                     max_size = Option.get_some_default
                                  100_000_000
                                  o_fragment_cache_size;
                     rocksdb_max_open_files =
                       Fragment_cache_config.default_rocksdb_max_open_files;
                     cache_on_read = true; cache_on_write = false;
                   })
       in
       let () = match cfg.chattiness with
         | None -> ()
         | Some _ -> Lwt_log.ign_warning "chattiness was deprecated, and won't be used"
       in

      Lwt_preemptive.set_bounds (lwt_preemptive_thread_pool_min_size,
                                 lwt_preemptive_thread_pool_max_size);

      verify_log_level log_level;

      Lwt_log.add_rule "*" (to_level log_level);
      Alba_arakoon.config_from_url albamgr_cfg_url >>= fun abm_cfg ->
      let abm_cfg_ref = ref abm_cfg in

      let _ : Lwt_unix.signal_handler_id =
        Lwt_unix.on_signal Sys.sigusr1 (fun _ ->
            let handle () =
              Lwt_log.info_f "Received USR1, reloading log level from config file" >>= fun () ->
              retrieve_cfg cfg_url >>= function
              | Result.Error err ->
                Lwt_log.info_f "Not reloading config as it could not be parsed"
              | Result.Ok cfg ->
                 Alba_arakoon.config_from_url albamgr_cfg_url >>= fun abm_cfg' ->
                 let () = abm_cfg_ref := abm_cfg' in
                 let log_level = cfg.Config.log_level in
                 Lwt_log.reset_rules ();
                 Lwt_log.add_rule "*" (to_level log_level);
                 Lwt_log.info_f "Reloaded albamgr config file + changed log level to %s" log_level
            in
            Lwt.ignore_result (Lwt_extra2.ignore_errors ~logging:true handle)) in

      Fragment_cache_config.make_fragment_cache fragment_cache_cfg
      >>= fun (fragment_cache, cache_on_read, cache_on_write) ->

      Proxy_server.run_server
        ips
        port
        ~transport
        abm_cfg_ref
        ~fragment_cache
        ~manifest_cache_size
        ~albamgr_connection_pool_size
        ~nsm_host_connection_pool_size
        ~osd_connection_pool_size
        ~osd_timeout
        ~albamgr_cfg_url
        ~max_client_connections
        ~tls_config:cfg.tls_client
        ~tcp_keepalive
        ~use_fadvise
        ~partial_osd_read:(match fragment_cache_cfg with
                           | Fragment_cache_config.None' -> true
                           | _ -> false)
        ~cache_on_read ~cache_on_write
        ~upload_slack
        ~read_preference
      >>= fun () ->

      fragment_cache # close ()
  in
  lwt_server ~log_sinks ~subcomponent:"proxy" t

let proxy_start_cmd =
  Term.(pure proxy_start
        $ Arg.(required
               & opt (some url_converter) None
               & info ["config"] ~docv:"CONFIG_FILE" ~doc:"proxy config file")
        $ log_sinks),
  Term.info "proxy-start" ~doc:"start a proxy server"

let proxy_client_cmd_line host port transport ~to_json ~verbose f =
  let t () =
    Proxy_client.with_client
      host port transport
      f
  in
  lwt_cmd_line ~to_json ~verbose t


let proxy_list_namespaces host port transport to_json verbose =
  proxy_client_cmd_line
    host port transport
    ~to_json ~verbose
    (fun client ->
       let rec inner acc cnt =
         let first, finc = match acc with
           | [] -> "", true
           | hd::_ -> hd, false in
         client # list_namespaces ~first ~finc ~last:None
                ~max:(-1) ~reverse:false
         >>= fun ((cnt', namespaces), has_more) ->
         let acc' = List.rev_append namespaces acc in
         let cnt'' = cnt' + cnt in
         if has_more
         then inner acc' cnt''
         else Lwt.return (cnt'', List.rev acc')
       in
       inner [] 0 >>= fun (cnt, namespaces) ->
       if to_json
       then
         print_result namespaces Alba_json.string_list_to_json
       else
         Lwt_io.printlf
           "Found %i namespaces: %s"
           cnt
           ([%show : string list] namespaces)
    )

let proxy_list_namespaces_cmd =
  Term.(pure proxy_list_namespaces
        $ host $ port 10000 $ transport
        $ to_json
        $ verbose),
  Term.info "proxy-list-namespaces" ~doc:"list namespaces"

let proxy_create_namespace
      host port transport namespace preset_name
      to_json verbose
  =
  proxy_client_cmd_line
    host port transport ~to_json ~verbose
    (fun client ->
      client # create_namespace ~namespace ~preset_name
      >>= unit_result to_json
    )

let proxy_create_namespace_cmd =
  Term.(pure proxy_create_namespace
        $ host
        $ port 10000
        $ transport
        $ namespace 0
        $ preset_name_namespace_creation 1
        $ to_json $ verbose),
  Term.info "proxy-create-namespace" ~doc:"create a namespace"


let proxy_upload_object host port transport namespace input_file object_name
                        allow_overwrite to_json verbose
                        unescape
  =
  let object_name = maybe_unescape unescape object_name in
  proxy_client_cmd_line
    host port transport ~to_json ~verbose
    (fun client ->
       client # write_object_fs
         ~namespace
         ~object_name
         ~input_file
         ~allow_overwrite ()
     >>= unit_result to_json
    )

let proxy_upload_object_cmd =
  Term.(pure proxy_upload_object
        $ host
        $ port 10000
        $ transport
        $ namespace 0
        $ file_upload 1
        $ object_name_upload 2
        $ allow_overwrite
        $ to_json $ verbose
        $ unescape
  ),
  Term.info "proxy-upload-object" ~doc:"upload an object to alba"

let proxy_download_object host port transport namespace object_name
                          output_file consistent_read
                          to_json verbose
                          unescape
  =
  let object_name = maybe_unescape unescape object_name in
  proxy_client_cmd_line
    host port transport ~to_json ~verbose
    (fun client ->
       client # read_object_fs
         ~namespace
         ~object_name
         ~output_file
         ~consistent_read
         ~should_cache:true
     >>= unit_result to_json
    )

let proxy_download_object_cmd =
  Term.(pure proxy_download_object
        $ host
        $ port 10000
        $ transport
        $ namespace 0
        $ object_name_download 1
        $ file_download 2
        $ consistent_read
        $ to_json $ verbose
        $ unescape
  ),
  Term.info "proxy-download-object" ~doc:"download an object from alba"

let proxy_delete_object
      host port transport namespace object_name
      to_json verbose unescape
  =
  let object_name = maybe_unescape unescape object_name in
  proxy_client_cmd_line
    host port transport ~to_json ~verbose
    (fun client ->
       client # delete_object
         ~namespace
         ~object_name
         ~may_not_exist:false
     >>= unit_result to_json
    )

let proxy_delete_object_cmd =
  Term.(pure proxy_delete_object
        $ host
        $ port 10000
        $ transport
        $ namespace 0
        $ object_name_upload 1
        $ to_json $ verbose
        $ unescape
  ),
  Term.info "proxy-delete-object" ~doc:"delete an object from alba"

let proxy_invalidate_cache host port transport namespace verbose =
  proxy_client_cmd_line
    host port transport ~to_json:false ~verbose
    (fun client ->
     client # invalidate_cache ~namespace
    )

let proxy_invalidate_cache_cmd =
  Term.(pure proxy_invalidate_cache
        $ host
        $ port 10000
        $ transport
        $ namespace 0
        $ verbose
  ),
  Term.info "proxy-invalidate-cache"
            ~doc:"invalidate the cache on the proxy for the namespace"

let proxy_statistics host port transport clear forget to_json verbose =
  let open Proxy_protocol in
  let request = { ProxyStatistics.clear ; forget } in
  proxy_client_cmd_line
    host port transport ~to_json ~verbose
    (fun client ->
     client # statistics request >>= fun stats ->
     if to_json
     then
       let open Alba_json.ProxyStatistics in
       print_result (make stats) to_yojson
     else
       let open Proxy_protocol in
       Lwt_io.printl ([%show: ProxyStatistics.t] stats)
    )

let proxy_statistics_cmd =
  let forget =
    let doc =
      "comma separated list of $(docv) that should be forgotten"
    in
    Arg.(value
         & opt (list string) []
         & info ["forget"] ~docv:"NAMESPACEs" ~doc
    )
  in
  Term.(pure proxy_statistics
        $ host $ port 10000 $ transport
        $ clear $ forget
        $ to_json $ verbose ),
  Term.info "proxy-statistics" ~doc:"retrieve statistics for this proxy"

let proxy_delete_namespace host port transport namespace to_json verbose =
  proxy_client_cmd_line host port transport ~to_json ~verbose
  (fun client ->
    client # delete_namespace ~namespace
    >>= unit_result to_json
  )

let proxy_delete_namespace_cmd =
  Term.(pure proxy_delete_namespace
        $ host $ port 10000 $ transport
        $ namespace 0
        $ to_json $ verbose),
  Term.info "proxy-delete-namespace" ~doc:"delete a namespace"

let proxy_list_objects
      host port transport
      namespace
      first finc last max reverse to_json verbose
  =
  let last = Option.map (fun l -> l, true ) last in
  proxy_client_cmd_line
    host port transport
    ~to_json ~verbose
   (fun client ->
    client # list_object ~namespace ~first ~finc ~last ~max ~reverse >>=
      fun (obj, hmore) ->
      if to_json
      then
        let result_to_json ((n,obj), (hmore:bool)) =
          `Assoc ["have_more", `Bool hmore;
                  "n", `Int n;
                  "objects" , Alba_json.string_list_to_json obj
                 ]
        in
        print_result (obj,hmore) result_to_json
      else
      Lwt_io.printl
        ([%show: (int * string list) * bool] (obj, hmore))
   )

let proxy_list_objects_cmd =
  Term.(pure proxy_list_objects
        $ host $ port 10000 $ transport
        $ namespace 0 $ first
        $ finc $ last $ max $ reverse
        $ to_json $ verbose),
  Term.info "proxy-list-objects" ~doc:"list the objects"

let proxy_get_version host port transport to_json verbose =
  proxy_client_cmd_line host port transport ~to_json ~verbose
    (fun client ->
      client # get_version
      >>= version_result to_json
    )

let proxy_get_version_cmd =
  Term.(pure proxy_get_version
        $ host
        $ port 10000
        $ transport
        $ to_json $ verbose
  ),
  Term.info "proxy-get-version" ~doc:"the proxy's version info"


let proxy_osd_view host port transport verbose =
  proxy_client_cmd_line
    host port transport ~to_json:false ~verbose
    (fun client ->
     client # osd_view >>= fun (claim,state_info) ->
     let ci,items = state_info in
     let cc,claims = claim in
     Lwt_io.printlf "claiminfo: %i items" cc >>= fun () ->
     Lwt_list.iter_s
       (fun (id, info) ->
        Lwt_io.printlf "%s:%s" id ([%show: Albamgr_protocol.Protocol.Osd.ClaimInfo.t ] info)
       ) claims

     >>= fun () ->
     Lwt_io.printlf "osd_info, state: %i entries:" ci >>= fun () ->
     let sorted = List.sort (fun (id0,_,_) (id1,_,_) -> Int64.compare id0 id1) items in
     Lwt_list.iter_s
       (fun (osd_id, info,state) ->
        Lwt_io.printlf "%Li:\n\t%s\n\t%s"
                       osd_id
                       (Nsm_model.OsdInfo.show info)
                       (Osd_state.show state)

       )
       sorted
    )

let proxy_osd_view_cmd =
  Term.(pure proxy_osd_view
        $ host $ port 10000 $transport
        $ verbose),
  Term.info "proxy-osd-view" ~doc:"this proxy's view on osds"

let proxy_osd_info host port transport verbose =
  proxy_client_cmd_line
    host port transport ~to_json:false ~verbose
    (fun client ->
     client # osd_info >>= fun (_, r) ->
     Lwt_io.printl
       ([%show :
            (Int64.t
             * Nsm_model.OsdInfo.t
             * Capabilities.OsdCapabilities.t)
              list]
          r)
    )

let proxy_osd_info_cmd =
  Term.(pure proxy_osd_info
        $ host $ port 10000 $transport
        $ verbose),
  Term.info "proxy-osd-info" ~doc:"request osd-info from the proxy"

let proxy_client_cfg host port transport verbose to_json =
  proxy_client_cmd_line
    host port transport ~to_json ~verbose
    (fun client ->
     client # get_client_config >>= fun cfg -> Alba_arakoon.Config.(
     if to_json
     then print_result (to_yojson cfg) (fun json -> json)
     else Lwt_io.printlf "client_cfg:\n%s" (show cfg)
         )
    )

let proxy_client_cfg_cmd =
  Term.(pure proxy_client_cfg
        $ host
        $ port 10000
        $ transport
        $ verbose
        $ to_json),
  Term.info "proxy-client-cfg" ~doc:"what the proxy thinks the albamgr client config is"

let extract_config url to_json verbose =
  let t () =
    Arakoon_config_url.retrieve url >>= fun txt ->
    if to_json
    then
      let json = Yojson.Safe.from_string txt in
      Lwt_io.printlf "%s" (Yojson.Safe.pretty_to_string json) >>= fun () ->
      Lwt.return_unit
    else
      Lwt_io.printlf "%s" txt
  in
  lwt_cmd_line ~to_json ~verbose t

let extract_config_cmd =
  Term.(pure extract_config
        $ alba_cfg_url
        $ to_json
        $ verbose
  ),
  Term.info "dev-extract-config" ~doc:"fetch and (pretty) print configuration"

let cmds = [
  proxy_start_cmd;
  proxy_list_namespaces_cmd;
  proxy_create_namespace_cmd;
  proxy_upload_object_cmd;
  proxy_download_object_cmd;
  proxy_delete_object_cmd;
  proxy_invalidate_cache_cmd;
  proxy_statistics_cmd;
  proxy_delete_namespace_cmd;
  proxy_list_objects_cmd;
  proxy_get_version_cmd;
  proxy_osd_view_cmd;
  proxy_client_cfg_cmd;
  proxy_osd_info_cmd;
  extract_config_cmd;
]
