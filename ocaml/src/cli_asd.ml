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

open Cmdliner
open Lwt
open Cli_common
open Checksum
open Slice
open Asd_protocol
open Asd_config


let asd_start cfg_url slow log_sinks =

  let t () =
    Asd_config.retrieve_cfg cfg_url >>= function
    | `Error err -> Lwt.fail_with err
    | `Ok cfg ->

       let ips,         port,      home,
           node_id,     log_level, asd_id,
           fsync,       limit,     multicast,
           buffer_size, tls,       tcp_keepalive,
           write_blobs,
           use_fadvise, use_fallocate
        =
        let open Config in
        cfg.ips,     cfg.port,      cfg.home,
        cfg.node_id, cfg.log_level, cfg.asd_id,
        cfg.__sync_dont_use, cfg.limit, cfg.multicast,
        cfg.buffer_size, cfg.tls,   cfg.tcp_keepalive,
        cfg.__warranty_void__write_blobs,
        cfg.use_fadvise,
        cfg.use_fallocate
      in

      (if not fsync
       then Lwt_log.warning "Fsync has been disabled, data will not be stored durably!!"
       else Lwt.return ()) >>= fun () ->
      (
        if port = None && tls = None
        then
          let msg = "neither port nor tls was configured" in
          Lwt_log.fatal msg>>= fun () ->
          Lwt.fail_with msg
        else Lwt.return ()
      ) >>= fun () ->
      verify_log_level log_level;
      Lwt_log.add_rule "*" (to_level log_level);

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
                let log_level = cfg.Config.log_level in
                Lwt_log.reset_rules ();
                Lwt_log.add_rule "*" (to_level log_level);
                Lwt_log.info_f "Changed log level to %s" log_level
            in
            Lwt.ignore_result (Lwt_extra2.ignore_errors ~logging:true handle)) in

      Asd_server.run_server ips port home ~asd_id ~node_id ~slow
                            ~fsync ~limit ~multicast ~buffer_size
                            ~tls
                            ~rocksdb_max_open_files:256
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
                 & info ["slow"] ~docv:"artifically slow down an asd (only for testing purposes!)")
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
    false verbose
    (fun () ->
     Asd_client.with_client
       buffer_pool
       ~conn_info asd_id
       f)

let with_osd_client (conn_info:Networking2.conn_info) osd_id f =
  Discovery.get_kind buffer_pool conn_info >>= function
  | None -> failwith "what kind is this?"
  | Some k ->
     let open Networking2 in
     Remotes.Pool.Osd.factory conn_info.tls_config buffer_pool k >>= fun (client, closer) ->
     Lwt.finalize
       (fun () -> f client)
       closer

let run_with_osd_client' conn_info osd_id verbose f =
  lwt_cmd_line
    false verbose
    (fun () -> with_osd_client conn_info osd_id f)

let asd_set hosts port tls_config asd_id (verbose:bool) key value =
  let conn_info = Networking2.make_conn_info hosts port tls_config in
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
                        $ hosts $ (port 8_000) $ tls_config
                        $ lido $ verbose
                        $ key $ value

                  ) in
  let info =
    let doc = "perform a set on a remote ASD" in
    Term.info "asd-set" ~doc in asd_set_t, info

let asd_get_version hosts port tls_config asd_id verbose =
  let conn_info = Networking2.make_conn_info hosts port tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client ->
     client # get_version () >>= fun (major,minor,patch, hash) ->
     Lwt_io.printlf "(%i, %i, %i, %S)" major minor patch hash
    )

let asd_get_version_cmd =
  let asd_get_version_t =
    Term.(pure asd_get_version
          $ hosts $ (port 8_000) $ tls_config
          $ lido $ verbose
    ) in
  let info = Term.info "asd-get-version"
                       ~doc:"get remote version"
  in
  asd_get_version_t, info

let asd_multi_get hosts port tls_config asd_id (keys:string list) verbose =
  let conn_info = Networking2.make_conn_info hosts port tls_config in
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
  let asd_multi_get_t = Term.(pure asd_multi_get
                              $ hosts $ (port 8_000) $ tls_config
                              $ lido
                              $ keys $ verbose)
  in
  let info =
    let doc = "perform a multi get on a remote ASD" in
    Term.info "asd-multi-get" ~doc
  in asd_multi_get_t, info


let asd_delete hosts port tls_config asd_id key verbose =
  let conn_info = Networking2.make_conn_info hosts port tls_config in
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client -> client # delete ~prio:Osd.High (Slice.wrap_string key))

let asd_delete_cmd =
  let doc = "$(docv)" in
  let key = Arg.(required &
                 pos 0 (some string) None &
                 info [] ~docv:"KEY" ~doc) in
  let asd_delete_t = Term.(pure asd_delete
                           $ hosts $ (port 8_000) $ tls_config
                           $ lido
                           $ key $ verbose)
  in
  let info =
    let doc = "perform a delete on a remote ASD" in
    Term.info "asd-delete" ~doc
  in asd_delete_t, info


let asd_range hosts port tls_config asd_id first verbose =
  let finc = true
  and last = None in
  let conn_info = Networking2.make_conn_info hosts port tls_config in
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
  let asd_range_t = Term.(pure asd_range
                          $ hosts $ (port 8_000) $tls_config
                          $ lido
                          $ first $ verbose) in
  let info =
    let doc = "range query on a remote ASD" in
    Term.info "asd-range" ~doc
  in
  asd_range_t, info

let osd_bench hosts port tls_config osd_id
              n_clients n
              value_size power prefix
              scenarios verbose
  =
  let conn_info = Networking2.make_conn_info hosts port tls_config in
  lwt_cmd_line
    false verbose
    (fun () ->
     Osd_bench.do_scenarios
       (fun f ->
        with_osd_client
          conn_info osd_id
          f)
       n_clients n
       value_size power prefix
       scenarios)

let osd_bench_cmd =
  let n_clients default =
    let doc = "number of concurrent clients for the benchmark" in
    Arg.(value
         & opt int default
         & info ["n-clients"] ~docv:"N_CLIENTS" ~doc)
  in
  let n default =
    let doc = "do runs (gets,sets,...) of $(docv) iterations" in
    Arg.(value
         & opt int default
         & info ["n"; "nn"] ~docv:"N" ~doc)
  in
  let value_size default =
    let doc = "do sets of $(docv) bytes" in
    Arg.(
      value
        & opt int default
        & info ["v"; "value-size"] ~docv:"V" ~doc
    )
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
  let scenarios =
    Arg.(let open Osd_bench in
         value
         & opt_all
             (enum
                [ "sets", sets;
                  "gets", gets;
                  "deletes", deletes;
                  "upload_fragments", upload_fragments;
                  "ranges",range_queries;
                ])
             [ sets;
               gets;
               deletes; ]
         & info [ "scenario" ])
  in
  let osd_bench_t = Term.(pure osd_bench
                          $ hosts $ port 10000 $ tls_config
                          $ lido
                          $ n_clients 1
                          $ n 10000
                          $ value_size 16384
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

let asd_statistics hosts port_o long_ids to_json verbose config_o tls_config clear =
  let open Asd_statistics in
  let _inner (client:Asd_client.client) = client # statistics clear in
  let process_results results =
    if to_json
    then
      begin
        let result_to_json rs =
          let x  =
            List.map
              (fun (long_id, stats) ->
                long_id, Alba_json.AsdStatistics.to_yojson stats
              ) rs
          in
          `Assoc x
        in
        print_result results result_to_json
      end
    else
      let f =
        (fun (long_id, stats) ->
          Lwt_io.printlf
            "%s : %s "
            long_id
            (AsdStatistics.show_inner
               stats
               Asd_protocol.Protocol.code_to_description
            )
        )
      in Lwt_list.iter_s f results
  in
  let from_asd hosts port tls_config verbose =
    begin
      let lido =
        match long_ids with
        | [] -> None
        | [long_id] -> Some long_id
        | _ -> failwith "cannot have more than one long-id with -h & -p"
      in
      let conn_info = Networking2.make_conn_info hosts port tls_config in
      let _inner' client =
        _inner client >>= fun stats ->
        let open Alba_json.AsdStatistics in
        if to_json
        then
          print_result (make stats) to_yojson
        else
          Lwt_io.printl
            (AsdStatistics.show_inner
               stats
               Asd_protocol.Protocol.code_to_description
            )
      in
      run_with_asd_client' ~conn_info lido verbose _inner'
    end
  in
  
  match port_o, config_o with
  | None, None -> failwith "specify either host & port or config"
  | None, Some cfg_file ->
     begin
       let t () =
         let open Nsm_model.OsdInfo in
         with_alba_client
           cfg_file tls_config
           (fun alba_client ->
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
                  get_conn_info k
                )
                stat_osds
            in
            Lwt_list.map_p
              (fun (long_id, conn_info) ->
                begin
                  Lwt.catch
                    (fun () ->
                      let conn_info = Asd_client.conn_info_from conn_info ~tls_config in
                      Asd_client.with_client
                        buffer_pool ~conn_info (Some long_id)
                        _inner >>= fun r ->
                      Some (long_id, r) |> Lwt.return
                    )
                    (fun exn ->
                      Lwt_log.info_f ~exn "couldn't reach %s" long_id >>= fun () ->
                      Lwt.return_none
                    )
                end
              ) needed_info
           )
         >>= fun results0 ->
         let rev_results =
           List.fold_left
             (fun acc ro -> match ro with None -> acc | Some r -> r :: acc )
             [] results0
         in 
         process_results (List.rev rev_results)
       in
       lwt_cmd_line to_json verbose t
     end
  | Some port,_ ->
     begin
       from_asd hosts port tls_config verbose
     end
       
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
                $ long_ids
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
         "{extras: %s ; ips = %s; port = %s ; tlsPort = %s }%!"
         extras_s
         ([%show: string list] record.ips)
         ([%show: int option] record.port)
         ([%show: int option] record.tlsPort)
  in
  lwt_cmd_line false verbose (fun () -> discovery seen)

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
  asd_set_full_cmd;
  asd_get_version_cmd;
  bench_syncfs_cmd;
  bench_fsync_cmd;
]
