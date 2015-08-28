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
open Lwt.Infix
open Cli_common

module Config = struct
  type t = {
    log_level : string;
    albamgr_cfg_file : string;
    albamgr_connection_pool_size : (int [@default 10]);
    nsm_host_connection_pool_size : (int [@default 10]);
    osd_connection_pool_size : (int [@default 10]);
    lwt_preemptive_thread_pool_min_size : (int [@default 6]);
    lwt_preemptive_thread_pool_max_size : (int [@default 8]);
    chattiness : float [@default 1.];
  } [@@deriving yojson, show]
end

let rec upload_albamgr_cfg albamgr_cfg (client : Alba_client.alba_client) =
  Lwt.catch
    (fun () ->
       client # mgr_access # store_client_config !albamgr_cfg >>= fun () ->
       Lwt.return `Done)
    (fun exn ->
       Lwt_log.info_f ~exn "Exception while uploading albamgr client config" >>= fun () ->
       Lwt.return `Retry) >>= function
  | `Done -> Lwt.return ()
  | `Retry ->
    Lwt_extra2.sleep_approx 60. >>= fun () ->
    upload_albamgr_cfg albamgr_cfg client



let alba_maintenance cfg_file modulo remainder flavour =
  let () =
    if modulo <= 0 ||
       remainder < 0 ||
       remainder >= modulo
    then failwith "bad modulo/remainder"
  in
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
  let filter id = (Int32.to_int id) mod modulo = remainder in

  let t () =
    read_cfg () >>= function
    | `Error err -> failwith err
    | `Ok cfg ->
      let open Config in
      let log_level                           = cfg.log_level
      and albamgr_cfg_file                    = cfg.albamgr_cfg_file
      and albamgr_connection_pool_size        = cfg.albamgr_connection_pool_size
      and nsm_host_connection_pool_size       = cfg.nsm_host_connection_pool_size
      and osd_connection_pool_size            = cfg.osd_connection_pool_size
      and lwt_preemptive_thread_pool_min_size = cfg.lwt_preemptive_thread_pool_min_size
      and lwt_preemptive_thread_pool_max_size = cfg.lwt_preemptive_thread_pool_max_size
      in

      Lwt_preemptive.set_bounds (lwt_preemptive_thread_pool_min_size,
                                 lwt_preemptive_thread_pool_max_size);

      verify_log_level log_level;

      Lwt_log.add_rule "*" (to_level log_level);

      let albamgr_cfg =
        ref (Albamgr_protocol.Protocol.Arakoon_config.from_config_file
               albamgr_cfg_file) in

      let albamgr_cfg_changed = Lwt_condition.create () in

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
                Lwt_condition.signal albamgr_cfg_changed ();

                let log_level = cfg.Config.log_level in
                Lwt_log.reset_rules();
                Lwt_log.add_rule "*" (to_level log_level);
                Lwt_log.info_f "Reloaded albamgr config file + changed log level to %s" log_level
            in
            Lwt.ignore_result (Lwt_extra2.ignore_errors handle)) in

      Lwt_log.info_f "maintenance version:%s" Alba_version.git_revision
      >>= fun () ->
      Lwt_log.info_f "Maintenance drone modulo:%i; remainder:%i" modulo remainder
      >>= fun () ->

      Alba_client.with_client
        albamgr_cfg
        ~albamgr_connection_pool_size
        ~nsm_host_connection_pool_size
        ~osd_connection_pool_size
        (fun client ->
           let maintenance_client =
             new Maintenance.client ~flavour ~filter (client # get_base_client)
           in
           Lwt.catch
             (fun () ->

                let rec inner () =
                  Lwt_condition.wait albamgr_cfg_changed >>= fun () ->
                  (* upload (the possible changed) client config to the albamgr *)
                  Lwt.ignore_result (upload_albamgr_cfg albamgr_cfg client);
                  inner ()
                in
                Lwt.ignore_result (inner ());

                (* upload current config at maintenance startup *)
                Lwt.ignore_result (upload_albamgr_cfg albamgr_cfg client);
                let base_threads () =
                  [
                    (Lwt_extra2.make_fuse_thread ());
                    (maintenance_client # deliver_all_messages ());
                    (client # get_base_client # discover_osds_check_claimed ());
                    (client # osd_access # propagate_osd_info ());
                  ]
                in

                let reporting_t =
                  Mem_stats.reporting_t
                    ~section:Lwt_log.Section.main
                    ()
                in

                let maintenance_threads =
                  let open Maintenance in
                  match flavour with
                  | NO_REBALANCE  (* handled inside maintenance client itself *)
                  | ALL_IN_ONE ->
                     (maintenance_client # do_work ())
                     :: (maintenance_client # maintenance_for_all_namespaces)
                     :: (maintenance_client # repair_osds)
                     :: base_threads ()
                  | ONLY_REBALANCE ->
                     (maintenance_client # maintenance_for_all_namespaces)
                     :: base_threads ()
                in
                let threads = reporting_t :: maintenance_threads in
                Lwt.pick threads

             )
             (fun exn ->
                Lwt_log.warning_f
                  ~exn
                  "Going down after unexpected exception in maintenance process"
                >>= fun () ->
                Lwt.fail exn))
  in
  lwt_server t

let alba_maintenance_cmd =
  let remainder default =
    let doc = "$(docv)" in
    Arg.(value
         & opt int default
         & info ["remainder"] ~docv:"REMAINDER" ~doc)
  in
  let modulo default =
    let doc = "$(docv)" in
    Arg.(value
         & opt int default
         & info ["modulo"] ~docv:"MODULO" ~doc)
  in
  let flavour =
    let open Maintenance in
    Arg.(value
         & vflag ALL_IN_ONE
                 [(ALL_IN_ONE,
                   info ["all-in-one"]
                        ~doc:"run all maintenance (including rebalancing)"
                  );
                  (ONLY_REBALANCE,
                   info ["only-rebalance"]
                        ~doc:"only run rebalance"
                  );
                  (NO_REBALANCE,
                   info ["no-rebalance"]
                        ~doc:"run all maintence except rebalance"
                  )
                 ]
    )
  in
  Term.(pure alba_maintenance
        $ Arg.(required
               & opt (some file) None
               & info ["config"] ~docv:"CONFIG_FILE" ~doc:"maintenance config file")      $ modulo 1
        $ remainder 0
        $ flavour
  ),
  Term.info "maintenance" ~doc:"run the maintenance process (garbage collection, obsolete fragment deletion, repair, ...)"

let alba_deliver_messages cfg_file =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         client # mgr_access # list_all_osds >>= fun (_, osds) ->
         let open Albamgr_protocol.Protocol in
         Lwt_list.iter_p
           (function
             | (Osd.ClaimInfo.ThisAlba osd_id, _) ->
                Lwt_extra2.ignore_errors
                  (fun () -> client # deliver_osd_messages ~osd_id)
             | _ -> Lwt.return ())
           osds >>= fun () ->

         client # mgr_access # list_all_nsm_hosts () >>= fun (_, nsms) ->
         Lwt_list.iter_p
           (fun (nsm_host_id, _, _) ->
            client # deliver_nsm_host_messages ~nsm_host_id)
           nsms
      )
  in
  lwt_cmd_line false t

let alba_deliver_messages_cmd =
  Term.(pure alba_deliver_messages
        $ alba_cfg_file),
  Term.info "deliver-messages" ~doc:"deliver all outstanding messages (note that this happens automatically by the maintenance process too, so you usually shouldn't need this.)"

let alba_rewrite_object
    cfg_file
    namespace
    object_name =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
         let maintenance_client = new Maintenance.client (client # get_base_client) in
         client # nsm_host_access # with_namespace_id ~namespace
           (fun namespace_id ->
              client # nsm_host_access # get_nsm_by_id ~namespace_id
              >>= fun nsm ->
              nsm # get_object_manifest_by_name object_name
              >>= function
              | None ->
                Lwt.fail_with
                  "No object with the specified name could be found"
              | Some manifest ->
                maintenance_client # repair_object_rewrite
                  ~namespace_id
                  ~manifest))
  in
  lwt_cmd_line false t

let alba_rewrite_object_cmd =
  Term.(pure alba_rewrite_object
        $ alba_cfg_file
        $ namespace 0
        $ object_name_upload 1),
  Term.info "rewrite-object" ~doc:"rewrite an object"

let cmds = [
  alba_maintenance_cmd;
  alba_deliver_messages_cmd;
  alba_rewrite_object_cmd;
]
