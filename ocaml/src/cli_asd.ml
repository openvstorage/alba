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
open Cli_common
open Cli_bench_common
open Checksum
open Slice
open Asd_protocol
open Asd_config

let asd_start cfg_url slow log_sinks =

  let t () =
    Asd_config.retrieve_cfg cfg_url >>= function
    | `Error err -> Lwt.fail_with err
    | `Ok cfg ->

       let ips,         port,      rora_port,
           transport, home,
           node_id,     log_level, asd_id,
           fsync,       limit,     capacity,
           multicast, buffer_size,
           tls_config,tcp_keepalive,
           write_blobs,
           use_fadvise, use_fallocate,
           rocksdb_block_cache_size
        =
        let open Config in
        cfg.ips,     cfg.port,      cfg.rora_port,
        cfg.transport, cfg.home,
        cfg.node_id, cfg.log_level, cfg.asd_id,
        cfg.__sync_dont_use,
        cfg.limit, cfg.capacity,
        cfg.multicast, cfg.buffer_size,
        cfg.tls,   cfg.tcp_keepalive,
        cfg.__warranty_void__write_blobs,
        cfg.use_fadvise, cfg.use_fallocate,
        cfg.rocksdb_block_cache_size
      in

      (if not fsync
       then Lwt_log.warning "Fsync has been disabled, data will not be stored durably!!"
       else Lwt.return ()) >>= fun () ->
      (
        if port = None && tls_config = None
        then
          let msg = "neither port nor tls was configured" in
          Lwt_log.fatal msg>>= fun () ->
          Lwt.fail_with msg
        else Lwt.return ()
      ) >>= fun () ->
      ( match transport with
        | "rdma" -> Lwt.return Net_fd.RDMA
        | "tcp"  -> Lwt.return Net_fd.TCP
        | x -> let msg = "transport needs to be `rdma` or `tcp`" in
               Lwt_log.fatal msg >>= fun () ->
               Lwt.fail_with msg
      ) >>= fun transport ->
      verify_log_level log_level;
      Lwt_log.add_rule "*" (to_level log_level);

      (if buffer_size <> None
       then Lwt_log.warning "Configured deprecated buffer_size argument -- ignoring!"
       else Lwt.return ()) >>= fun () ->

      let get_capacity = function
       | Some c -> c
       | None ->
          let _, c = Fsutil.disk_usage home in
          c
      in

      let capacity = ref (get_capacity capacity) in

      let _ : Lwt_unix.signal_handler_id =
        Lwt_unix.on_signal Sys.sigusr1 (fun _ ->
            let handle () =
              Lwt_log.info_f "Received USR1, refetching log level from config %s"
                             (Prelude.Url.show cfg_url)
              >>= fun () ->
              Asd_config.retrieve_cfg cfg_url >>= function
              | `Error err ->
                Lwt_log.info_f "Not reloading config as it could not be parsed"
              | `Ok cfg ->

                capacity := get_capacity cfg.Config.capacity;

                let log_level = cfg.Config.log_level in
                Lwt_log.reset_rules ();
                Lwt_log.add_rule "*" (to_level log_level);

                Lwt_log.info_f "Reloaded capacity & log level"
            in
            Lwt.ignore_result (Lwt_extra2.ignore_errors ~logging:true handle)) in

      Asd_server.run_server ips ~port ~rora_port
                            ~transport
                            home ~asd_id ~node_id ~slow
                            ~fsync
                            ~limit
                            ~capacity
                            ~multicast
                            ~tls_config
                            ~rocksdb_max_open_files:256
                            ~rocksdb_recycle_log_file_num:None
                            ~rocksdb_block_cache_size
                            ~tcp_keepalive
                            ~write_blobs
                            ~use_fadvise
                            ~use_fallocate
  in

  lwt_server ~log_sinks ~subcomponent:"asd" t

let asd_start_cmd =
  let cfg_url =
    Arg.(required
         & opt (some url_converter) None
         & info ["config"] ~docv:"CONFIG_FILE" ~doc:"asd config uri")
  in
  let asd_start_t =
    Term.(pure asd_start
          $ cfg_url
          $ Arg.(value
                 & flag
                 & info ["slow"] ~doc:"artifically slow down an asd (only for testing purposes!)")
          $ log_sinks)
  in
  let info =
    Term.info
      "asd-start"
      ~doc:"start ASD server"
  in
  asd_start_t, info

let buffer_pool = Buffer_pool.osd_buffer_pool

let run_with_asd_client' ~conn_info asd_id verbose f =
  lwt_cmd_line
    ~to_json:false ~verbose
    (fun () ->
     Asd_client.with_client
       ~conn_info asd_id
       f)

let with_osd_client (conn_info:Networking2.conn_info) osd_id f =
  Discovery.get_kind buffer_pool conn_info >>= function
  | None -> failwith "what kind is this?"
  | Some k ->
     let open Networking2 in
     Osd_access.Osd_pool.factory
       conn_info.tls_config
       Tcp_keepalive2.default
       buffer_pool
       (Alba_osd.make_client ~albamgr_connection_pool_size:10)
       k >>= fun (client, closer) ->
     Lwt.finalize
       (fun () -> f client)
       closer

let run_with_osd_client' conn_info osd_id verbose f =
  lwt_cmd_line
    ~to_json:false ~verbose
    (fun () -> with_osd_client conn_info osd_id f)

let asd_set hosts port transport tls_config asd_id (verbose:bool) key value =
  let conn_info = Networking2.make_conn_info hosts port ~transport tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client ->
     let checksum = Checksum.NoChecksum in
     Lwt_log.warning "checksum option will be `NoChecksum`"
     >>= fun ()->
     client # apply_sequence ~prio:Osd.High []
             [ Update.set
                 (Slice.wrap_string key)
                 (Osd.Blob.Bytes value)
                 checksum true
             ]
    )

let asd_set_cmd =
  let doc = "$(docv)" in
  let key = Arg.(required &
                 pos 0 (some string) None &
                 info [] ~docv:"KEY" ~doc) in
  let value = Arg.(required &
                   pos 1 (some string) None &
                   info [] ~docv:"VALUE" ~doc) in
  let asd_set_t = Term.(pure asd_set
                        $ hosts $ (port 8_000)
                        $ transport
                        $ tls_config
                        $ lido $ verbose
                        $ key $ value

                  ) in
  let info =
    let doc = "perform a set on a remote ASD" in
    Term.info "asd-set" ~doc in asd_set_t, info

let asd_get_version hosts port transport tls_config asd_id verbose =
  let conn_info = Networking2.make_conn_info hosts port ~transport tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client ->
     client # get_version () >>= fun (major,minor,patch, hash) ->
     Lwt_io.printlf "(%i, %i, %i, %S)" major minor patch hash
    )

let asd_get_version_cmd =
  let asd_get_version_t =
    Term.(pure asd_get_version
          $ hosts $ (port 8_000)
          $ transport
          $ tls_config
          $ lido $ verbose
    ) in
  let info = Term.info "asd-get-version"
                       ~doc:"get remote version"
  in
  asd_get_version_t, info

let asd_multi_get hosts port transport tls_config asd_id (keys:string list) verbose =
  let conn_info = Networking2.make_conn_info hosts port ~transport tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client ->

     client # multi_get ~prio:Osd.High (List.map Slice.wrap_string keys)
     >>= fun values ->
     print_endline ([%show: (Lwt_bytes2.Lwt_bytes.t * Checksum.t) option list] values);
     Lwt.return ())

let asd_multi_get_cmd =
  let doc = "$(docv)" in
  let keys = Arg.(non_empty &
                 pos_all string [] &
                 info [] ~docv:"KEYS" ~doc) in
  let asd_multi_get_t =
    Term.(pure asd_multi_get
          $ hosts $ (port 8_000) $ transport
          $ tls_config
          $ lido
          $ keys $ verbose)
  in
  let info =
    let doc = "perform a multi get on a remote ASD" in
    Term.info "asd-multi-get" ~doc
  in asd_multi_get_t, info


let asd_delete hosts port transport tls_config asd_id key verbose =
  let conn_info = Networking2.make_conn_info hosts port ~transport tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client -> client # delete ~prio:Osd.High (Slice.wrap_string key))

let asd_delete_cmd =
  let doc = "$(docv)" in
  let key = Arg.(required &
                 pos 0 (some string) None &
                 info [] ~docv:"KEY" ~doc) in
  let asd_delete_t =
    Term.(pure asd_delete
          $ hosts $ (port 8_000) $ transport
          $ tls_config
          $ lido
          $ key $ verbose)
  in
  let info =
    let doc = "perform a delete on a remote ASD" in
    Term.info "asd-delete" ~doc
  in asd_delete_t, info

let asd_disk_usage hosts port transport tls_config asd_id verbose =
  let conn_info = Networking2.make_conn_info hosts port ~transport tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client ->
      client # get_disk_usage () >>= fun (used,cap) ->
     Lwt_io.printlf "disk_usage:(%Li,%Li)" used cap
    )

let asd_disk_usage_cmd =
  let asd_disk_usage_t =
    Term.(pure asd_disk_usage
          $ hosts $ port 8000 $ transport $tls_config
          $ lido
          $ verbose
    )
  in
  let info =
    let doc = "return ASD disk usage (used,cap)" in
    Term.info "asd-disk-usage" ~doc
  in
  asd_disk_usage_t, info

let asd_range hosts port transport tls_config asd_id first verbose =
  let finc = true
  and last = None in
  let conn_info = Networking2.make_conn_info hosts port ~transport tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client ->
     client # range
            ~prio:Osd.High
            ~first:(Slice.wrap_string first)
            ~finc ~last ~reverse:false ~max:~-1
     >>= fun ((cnt, keys), has_more) ->
     Lwt_io.printlf
       "range ~first:%S ~finc:%b ~last:None ~reverse:false has_more=%b, cnt=%i, keys=%s"
       first finc
       has_more
       cnt
       ([%show : Slice.t list] keys)
    )

let asd_range_cmd =
  let first =
    let doc = "start key" in
    Arg.(value
         & opt string ""
         & info ["f";"first"] ~docv:"FIRST" ~doc)
  in
  let asd_range_t =
    Term.(pure asd_range
          $ hosts $ (port 8_000) $ transport $ tls_config
          $ lido
          $ first $ verbose)
  in
  let info =
    let doc = "range query on a remote ASD" in
    Term.info "asd-range" ~doc
  in
  asd_range_t, info

let osd_bench hosts port transport tls_config osd_id
              n_clients n
              value_size partial_fetch_size
              power prefix
              scenarios verbose
  =
  let conn_info =
    Networking2.make_conn_info hosts port ~transport tls_config
  in
  lwt_cmd_line
    ~to_json:false ~verbose
    (fun () ->
     Osd_bench.do_scenarios
       (fun f ->
        with_osd_client
          conn_info osd_id
          f)
       n_clients n
       value_size partial_fetch_size
       power prefix
       scenarios)

let osd_bench_cmd =
  let scenarios =
    Arg.(let open Osd_bench in
         let scns = [ "sets", sets;
                      "gets", gets;
                      "deletes", deletes;
                      "upload_fragments", upload_fragments;
                      "ranges",range_queries;
                      "partial_reads", partial_reads;
                      "get_version", get_version;
                      "exists", exists;
                    ]
         in
         value
         & opt_all
             (enum scns)
             [ sets;
               gets;
               deletes; ]
         & info [ "scenario" ]
                ~doc:(Printf.sprintf
                        "choose which scenario to run, valid values are %s"
                        ([%show : string list] (List.map fst scns))))
  in
  let osd_bench_t =
    Term.(pure osd_bench
          $ hosts $ port 10000 $ transport
          $ tls_config
          $ lido
          $ n_clients 1
          $ n 10000
          $ value_size 16384
          $ partial_fetch_size 4096
          $ power 4
          $ prefix ""
          $ scenarios
          $ verbose
    )
  in
  let info =
    Term.info
      "osd-benchmark"
      ~doc:"perform a benchmark on an osd"
  in
  osd_bench_t, info

let asd_osd_info_from_kind k =
  let open Nsm_model.OsdInfo in
  match k with
  | Asd (x, _) -> x
  | Kinetic _ | Alba _ -> assert false

let asd_multistatistics long_ids to_json verbose cfg_file tls_config clear =
  begin
    let process_results results =
      if to_json
      then
        begin
          let result_to_json rs =
            let x  =
              List.map
                (fun (long_id, stats_result) ->
                  let result =
                    let open Alba_json in
                    let open Prelude.Error in
                    let open Result in
                    match stats_result with
                    | Ok (stats, disk_usage)   ->
                       to_yojson
                         AsdStatistics.to_yojson
                         { success=true ;
                           result= (stats, disk_usage);
                         }
                    | Error exn  ->
                       to_yojson
                         (fun exn -> `String (Printexc.to_string exn))
                         { success=false; result= exn}
                  in
                  long_id, result
                ) rs
            in
            `Assoc x
          in
          print_result results result_to_json
        end
      else
        let f =
          (fun (long_id, stats_result) ->
            let open Prelude.Error in
            let v = match stats_result with
              | Ok (stats, disk_usage) ->
                 Printf.sprintf
                   "stats: %s, disk_usage:(%Li,%Li)"
                   (Asd_statistics.AsdStatistics.show_inner
                      stats
                      Asd_protocol.Protocol.code_to_description)
                   (fst disk_usage)
                   (snd disk_usage)
              | Error exn -> Printexc.to_string exn
            in
            Lwt_io.printlf "%s : %s " long_id v
          )
      in Lwt_list.iter_s f results
    in
    let t () =
      with_alba_client
        cfg_file tls_config
        (fun alba_client ->
          let open Nsm_model.OsdInfo in
          alba_client # mgr_access # list_all_claimed_osds >>= fun (_n, osds) ->
          let stat_osds =
            List.filter
              (fun (_,osd_info) ->
                let k = osd_info.kind in
                List.mem (get_long_id k) long_ids
              ) osds
          in

          let needed_info =
            List.map
              (fun (_,osd_info) ->
                let k = osd_info.kind in
                get_long_id k,
                asd_osd_info_from_kind k
              )
              stat_osds
          in
          Lwt_list.map_p
            (fun (long_id, conn_info) ->
              begin
                Lwt.catch
                  (fun () ->
                   Lwt_unix.with_timeout
                     1.
                     (fun () ->
                      let conn_info = Asd_client.conn_info_from conn_info ~tls_config in
                      Asd_client.with_client
                        ~conn_info (Some long_id)
                        (fun client ->
                         client # statistics clear >>= fun stats ->
                         client # get_disk_usage () >>= fun disk_usage ->
                         Lwt.return (stats, disk_usage))
                      >>= fun r ->
                      (long_id, Prelude.Error.Ok r) |> Lwt.return)
                  )
                  (fun exn ->
                    Lwt_log.info_f ~exn "couldn't reach %s" long_id >>= fun () ->
                    Lwt.return (long_id, Prelude.Error.Error exn)
                  )
              end
            ) needed_info
        )
      >>= fun results ->
      process_results results
    in
    lwt_cmd_line ~to_json ~verbose t
  end

let asd_multistatistics_cmd =
  let t = Term.(pure asd_multistatistics
                $ long_ids
                $ to_json
                $ verbose
                $ alba_cfg_url
                $ tls_config
                $ clear
          )
  in
  let info = Term.info "asd-multistatistics" ~doc:"get statistics from many asds" in
  t, info

let asd_statistics hosts port_o transport asd_id
                   to_json verbose config_o tls_config clear
  =
  let open Asd_statistics in
  let _inner =
    (fun (client:Asd_client.client) ->
     client # statistics clear >>= fun stats ->
     client # get_disk_usage () >>= fun disk_usage ->
     if to_json
     then
       let open Alba_json.AsdStatistics in
       print_result (make stats, disk_usage) to_yojson
     else Lwt_io.printl
            (AsdStatistics.show_inner
               stats
               Asd_protocol.Protocol.code_to_description)
    )
  in
  let from_asd hosts port transport tls_config asd_id verbose =
    begin
      let conn_info = Networking2.make_conn_info hosts port ~transport tls_config in
      run_with_asd_client' ~conn_info asd_id verbose _inner
    end
  in
  match port_o, config_o with
  | None, None -> failwith "specify either host & port or config"
  | None, Some cfg_file ->
     begin
       let t () =
         with_alba_client
           cfg_file tls_config
           (
             (*(fun alba_client -> client # with_osd osd_id _inner) *)
            fun alba_client ->
              let asd_id_v = Prelude.Option.get_some asd_id in
              alba_client # mgr_access # get_osd_by_long_id ~long_id:asd_id_v
              >>= fun r_o ->
              begin
                match r_o with
                | None ->
                  let msg = Printf.sprintf
                      "%s: asd not found" asd_id_v
                  in
                  Lwt.fail (Failure msg)
                | Some (claim_info, osd) ->
                  let conn_info =
                    let open Nsm_model.OsdInfo in
                    let conn_info' = asd_osd_info_from_kind osd.kind in
                    Asd_client.conn_info_from conn_info' ~tls_config
                  in
                  Asd_client.with_client
                    ~conn_info asd_id
                    _inner
              end
           )
       in
       lwt_cmd_line ~to_json ~verbose t
     end
  | Some port,_ -> from_asd hosts port transport tls_config asd_id verbose

let asd_statistics_cmd =
  let port_o =
    Arg.(value
         & opt (some int) None
         & info ["p";"port"] ~docv:"PORT" ~doc:"port"
    )
  in
  let config_o =
    Arg.(value
         & opt (some url_converter) None
         & info ["config"] ~docv:"CONFIG" ~doc:"the alba mgr config url (arakoon cfg)"
    )
  in

  let t = Term.(pure asd_statistics
                $ hosts
                $ port_o
                $ transport
                $ lido
                $ to_json
                $ verbose
                $ config_o
                $ tls_config
                $ clear
          )
  in
  let info = Term.info "asd-statistics" ~doc:"get statistics from the asd" in
  t, info


let asd_discover verbose () =
  let open Discovery in
  let seen = function
    | Bad s -> Lwt_io.printlf "bad json:\n%S" s
    | Good (_, record) ->
       let extras_s = [%show : extra_info option ]
                       record.extras
       in
       Lwt_io.printlf
         "{extras: %s ; ips = %s; port = %s ; tlsPort = %s ; useRdma = %s ; }%!"
         extras_s
         ([%show: string list] record.ips)
         ([%show: int option] record.port)
         ([%show: int option] record.tlsPort)
         ([%show: bool ] record.useRdma)
  in
  lwt_cmd_line ~to_json:false ~verbose (fun () -> discovery seen)

let asd_discover_cmd =
  let term = Term.(pure asd_discover $ verbose $ pure ()) in
  let info = Term.info "asd-discovery" ~doc:"listen for ASD servers" in
  term,info


let asd_set_full hosts port tls_config asd_id full verbose =
  let conn_info = Networking2.make_conn_info hosts port tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client -> client # set_full full)


let asd_set_full_cmd =
  let doc = "$(docv)" in
  let full =
    Arg.(value
         & pos 0 bool true
         & info [] ~docv:"FULL" ~doc)
  in
  let asd_set_full_t =
    Term.(pure asd_set_full
          $ hosts $ (port 8_000) $ tls_config
          $ lido
          $ full $ verbose
    )
  in
  let info =
    let doc = "make this asd behave as if (not) full" in
    Term.info "asd-set-full" ~doc
  in
  asd_set_full_t, info

let bench_syncfs root iterations threads size =
  let t =
    let entry = Sync_bench.batch_entry_syncfs in
    let post_batch dir_fd = Syncfs.lwt_syncfs dir_fd in
    Sync_bench.bench_x root entry post_batch iterations threads size
    in
  Lwt_main.run t

let bench_fsync root iterations threads size =
  let t =
    let entry = Sync_bench.batch_entry_fsync in
    let post_batch dir_fd = Lwt.return 0 in
    Sync_bench.bench_x root entry post_batch iterations threads size
  in
  Lwt_main.run t

let size =
  let doc = "$docv" in
  Arg.(value
       & opt int (900*1024)
       & info ["size"] ~docv:"SIZE" ~doc
  )

let threads =
  let doc = "$docv" in
  Arg.(value
       & opt int 10
       & info ["threads"] ~docv:"n threads" ~doc
  )
let iterations =
  let doc = "$docv" in
  Arg.(value
       & opt int 100
       & info ["iterations"] ~docv:"iterations" ~doc
  )

let root =
  let doc = "$docv" in
  Arg.(value
       & opt dir "/tmp/bench_x"
       & info ["root"] ~docv:"root" ~doc
  )
let bench_syncfs_cmd =
  Term.(pure bench_syncfs
        $ root $ iterations $threads $ size
  ),
  Term.info "bench-syncfs" ~doc:"write files and sync with syncfs"

let bench_fsync_cmd =
  Term.(pure bench_fsync
        $ root $ iterations $threads $ size
  ),
  Term.info "bench-fsync" ~doc:"write files and sync with fsync"


let cmds = [
  asd_start_cmd;
  asd_set_cmd;
  asd_multi_get_cmd;
  asd_delete_cmd;
  asd_range_cmd;
  osd_bench_cmd;
  asd_discover_cmd;
  asd_statistics_cmd;
  asd_multistatistics_cmd;
  asd_set_full_cmd;
  asd_get_version_cmd;
  asd_disk_usage_cmd;
  bench_syncfs_cmd;
  bench_fsync_cmd;
  (* Asd_kaboom.kaboom_cmd; *)
]
