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

open Lwt
open Cmdliner
open Cli_common
open Prelude

module Config = struct
  type t = {
    ips : (string list [@default []]);
    port : int;
    log_level : string;
    albamgr_cfg_file : string;
    fragment_cache_dir : string;
    manifest_cache_size : (int [@default 100_000]);
    fragment_cache_size : (int [@default 100_000_000]);
    albamgr_connection_pool_size : (int [@default 10]);
    nsm_host_connection_pool_size : (int [@default 10]);
    osd_connection_pool_size : (int [@default 10]);
    lwt_preemptive_thread_pool_min_size : (int [@default 6]);
    lwt_preemptive_thread_pool_max_size : (int [@default 8]);
    chattiness : float [@default 1.0];
  } [@@deriving yojson, show]
end


let proxy_start cfg_file =
  let read_cfg () =
    read_file cfg_file >>= fun txt ->
    Lwt_log.info_f "Found the following config: %s" txt >>= fun () ->
    let config = Config.of_yojson (Yojson.Safe.from_string txt) in
    (match config with
     | `Error err ->
       Lwt_log.warning_f "Error while parsing cfg file: %s" err
     | `Ok cfg ->
       Lwt_log.info_f
         "Interpreted the config as: %s"
         ([%show : Config.t] cfg)) >>= fun () ->
    Lwt.return config
  in
  let t () =
    read_cfg () >>= function
    | `Error err -> failwith err
    | `Ok cfg ->
      let open Config in
      let ips  = cfg.ips
      and port = cfg.port
      and log_level = cfg.log_level
      and albamgr_cfg_file = cfg.albamgr_cfg_file
      and
        cache_dir,
        manifest_cache_size,
        fragment_cache_size,
        albamgr_connection_pool_size,
        nsm_host_connection_pool_size,
        osd_connection_pool_size,
        lwt_preemptive_thread_pool_min_size, lwt_preemptive_thread_pool_max_size =
        cfg.fragment_cache_dir,
        cfg.manifest_cache_size,
        (* the fragment cache size is currently a rather soft limit which we'll
           surely exceed. this can lead to disk full conditions. by taking a
           safety margin of 10% we turn the soft limit into a hard one... *)
        cfg.fragment_cache_size / 10 * 9,
        cfg.albamgr_connection_pool_size,
        cfg.nsm_host_connection_pool_size,
        cfg.osd_connection_pool_size,
        cfg.lwt_preemptive_thread_pool_min_size, cfg.lwt_preemptive_thread_pool_max_size
      and chattiness = cfg.chattiness
      in

      Lwt_preemptive.set_bounds (lwt_preemptive_thread_pool_min_size,
                                 lwt_preemptive_thread_pool_max_size);

      assert (cache_dir.[0] = '/');
      verify_log_level log_level;

      Lwt_log.add_rule "*" (to_level log_level);

      let albamgr_cfg =
        ref (Albamgr_protocol.Protocol.Arakoon_config.from_config_file
               albamgr_cfg_file) in

      let _ : Lwt_unix.signal_handler_id =
        Lwt_unix.on_signal Sys.sigusr1 (fun _ ->
            let handle () =
              Lwt_log.info_f "Received USR1, reloading log level from config file" >>= fun () ->
              read_cfg () >>= function
              | `Error err ->
                Lwt_log.info_f "Not reloading config as it could not be parsed"
              | `Ok cfg ->
                albamgr_cfg :=
                  Albamgr_protocol.Protocol.Arakoon_config.from_config_file
                    albamgr_cfg_file;
                let log_level = cfg.Config.log_level in
                Lwt_log.reset_rules ();
                Lwt_log.add_rule "*" (to_level log_level);
                Lwt_log.info_f "Reloaded albamgr config file + changed log level to %s" log_level
            in
            Lwt.ignore_result (Lwt_extra2.ignore_errors handle)) in

      Proxy_server.run_server
        ips
        port
        cache_dir
        albamgr_cfg
        ~manifest_cache_size
        ~fragment_cache_size
        ~albamgr_connection_pool_size
        ~nsm_host_connection_pool_size
        ~osd_connection_pool_size
        ~albamgr_cfg_file
        ~chattiness
  in
  lwt_server t

let proxy_start_cmd =
  Term.(pure proxy_start
        $ Arg.(required
               & opt (some file) None
               & info ["config"] ~docv:"CONFIG_FILE" ~doc:"proxy config file")),
  Term.info "proxy-start" ~doc:"start a proxy server"

let proxy_client_cmd_line host port f =
  let t () =
    Proxy_client.with_client
      host port
      f
  in
  lwt_cmd_line false t


let proxy_list_namespaces host port =
  proxy_client_cmd_line
    host port
    (fun client ->
       let rec inner acc cnt =
         let first, finc = match acc with
           | [] -> "", true
           | hd::_ -> hd, false in
         client # list_namespace ~first ~finc ~last:None
                ~max:(-1) ~reverse:false
         >>= fun ((cnt', namespaces), has_more) ->
         let acc' = List.rev_append namespaces acc in
         let cnt'' = cnt' + cnt in
         if has_more
         then inner acc' cnt''
         else Lwt.return (cnt'', List.rev acc')
       in
       inner [] 0 >>= fun (cnt, namespaces) ->
       Lwt_io.printlf
         "Found %i namespaces: %s"
         cnt
         ([%show : string list] namespaces)
    )

let proxy_list_namespaces_cmd =
  Term.(pure proxy_list_namespaces
        $ host
        $ port 10000),
  Term.info "proxy-list-namespaces" ~doc:"list namespaces"

let proxy_create_namespace host port namespace preset_name =
  proxy_client_cmd_line
    host port
    (fun client ->
       client # create_namespace ~namespace ~preset_name)

let proxy_create_namespace_cmd =
  Term.(pure proxy_create_namespace
        $ host
        $ port 10000
        $ namespace 0
        $ preset_name_namespace_creation 1),
  Term.info "proxy-create-namespace" ~doc:"create a namespace"


let proxy_upload_object host port namespace input_file object_name allow_overwrite =
  proxy_client_cmd_line
    host port
    (fun client ->
       client # write_object_fs
         ~namespace
         ~object_name
         ~input_file
         ~allow_overwrite ())

let proxy_upload_object_cmd =
  Term.(pure proxy_upload_object
        $ host
        $ port 10000
        $ namespace 0
        $ file_upload 1
        $ object_name_upload 2
        $ allow_overwrite),
  Term.info "proxy-upload-object" ~doc:"upload an object to alba"

let proxy_download_object host port namespace object_name
                          output_file consistent_read =
  proxy_client_cmd_line
    host port
    (fun client ->
       client # read_object_fs
         ~namespace
         ~object_name
         ~output_file
         ~consistent_read
         ~should_cache:true
    )

let proxy_download_object_cmd =
  Term.(pure proxy_download_object
        $ host
        $ port 10000
        $ namespace 0
        $ object_name_download 1
        $ file_download 2
        $ consistent_read
  ),
  Term.info "proxy-download-object" ~doc:"download an object from alba"

let proxy_invalidate_cache host port namespace =
  proxy_client_cmd_line
    host port
    (fun client ->
     client # invalidate_cache ~namespace
    )

let proxy_invalidate_cache_cmd =
  Term.(pure proxy_invalidate_cache
        $ host
        $ port 10000
        $ namespace 0
  ),
  Term.info "proxy-invalidate-cache"
            ~doc:"invalidate the cache on the proxy for $(NAMESPACE)"

let proxy_statistics host port clear to_json =
  proxy_client_cmd_line
    host port
    (fun client ->
     client # statistics clear >>= fun stats ->
     if to_json
     then
       let open Alba_json.ProxyStatistics in
       print_result (make stats) to_yojson
     else
       let open Proxy_protocol in
       Lwt_io.printl ([%show: ProxyStatistics.t] stats)
    )

let proxy_statistics_cmd =
  Term.(pure proxy_statistics $ host $ port 10000 $ clear $ to_json),
  Term.info "proxy-statistics" ~doc:"retrieve statistics for this proxy"

let proxy_delete_namespace host port namespace =
  proxy_client_cmd_line host port
  (fun client ->
   client # delete_namespace ~namespace >>= fun () ->
   Lwt_io.printl (namespace ^ " is deleted"))

let proxy_delete_namespace_cmd =
  Term.(pure proxy_delete_namespace $ host $ port 10000 $ namespace 0),
  Term.info "proxy-delete-namespace" ~doc:"delete a namespace"

let proxy_list_objects host port namespace
                      first finc last max reverse =
  let last = Option.map (fun l -> l, true ) last in
  proxy_client_cmd_line host port
   (fun client ->
    client # list_object ~namespace ~first ~finc ~last ~max ~reverse >>=
      fun (obj, hmore) ->
      let open Std in  Lwt_io.printl
                         ([%show: (int * string list) * bool] (obj, hmore)))

let proxy_list_objects_cmd =
  Term.(pure proxy_list_objects $ host $ port 10000 $ namespace 0 $ first $
          finc $ last $ max $ reverse),
  Term.info "proxy-list-objects" ~doc:"list the objects"

let proxy_get_version host port =
  proxy_client_cmd_line host port
    (fun client ->
     client # get_version >>= fun (major,minor,patch, hash) ->
     Lwt_io.printlf "(%i, %i, %i, %S)" major minor patch hash
    )

let proxy_get_version_cmd =
  Term.(pure proxy_get_version
        $ host
        $ port 10000
  ),
  Term.info "proxy-get-version" ~doc:"the proxy's version info"

let proxy_bench host port
                (n:int) file_name (power:int)
                prefix (slice_size:int) namespace_name
  =
  lwt_cmd_line
    false
    (fun () -> Proxy_bench.bench host port n file_name power prefix slice_size namespace_name)


let proxy_bench_cmd =
  let n default =
    let doc = "do runs (writes,reads,partial_reads,...) of $(docv) iterations" in
    Arg.(value
         & opt int default
         & info ["n"; "nn"] ~docv:"N" ~doc)
  in
  let power default =
    let doc = "$(docv) for random number generation: period = 10^$(docv)" in
    Arg.(value
           & opt int default
           & info ["power"] ~docv:"power" ~doc
    )
  in
  let prefix default =
    let doc = "$(docv) to keep multiple clients out of each other's way" in
    Arg.(value
           & opt string default
           & info ["prefix"] ~docv:"prefix" ~doc
    )
  in
  let slice_size default =
    let doc = "partial reads phaze will use slices of size $(docv)" in
    Arg.(value
         & opt int default
         & info ["slice-size"] ~docv:"SLICE_SIZE"  ~doc
    )
  in
  Term.(pure proxy_bench
        $ host $ port 10000
        $ n 10000
        $ file_upload 1
        $ power 4
        $ prefix ""
        $ slice_size 4096
        $ namespace 0
  ),
  Term.info "proxy-bench" ~doc:"simple proxy benchmark"
let cmds = [
  proxy_start_cmd;
  proxy_list_namespaces_cmd;
  proxy_create_namespace_cmd;
  proxy_upload_object_cmd;
  proxy_download_object_cmd;
  proxy_invalidate_cache_cmd;
  proxy_statistics_cmd;
  proxy_delete_namespace_cmd;
  proxy_list_objects_cmd;
  proxy_get_version_cmd;
  proxy_bench_cmd
]
