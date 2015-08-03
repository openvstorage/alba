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
open Cli_common
open Checksum
open Slice
open Asd_protocol

module Config = struct
  type t = {
    ips : string list [@default []];
    port : int;
    node_id : string;
    home : string;
    log_level : string;
    asd_id : string option  [@default None];
    __sync_dont_use : bool  [@default true];
    limit : int64           [@default 99L];
    buffer_size : int       [@default 8192];
    multicast: float option [@default (Some 10.0)];
  } [@@deriving yojson, show]
end


let asd_start cfg_file slow =
  let read_cfg () =
    read_file cfg_file >>= fun txt ->
    Lwt_log.debug_f "Found the following config: %s" txt >>= fun () ->
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

      let ips, port, home, node_id, log_level, asd_id,
          fsync, limit, multicast, buffer_size
        =
        let open Config in
        cfg.ips, cfg.port,
        cfg.home,
        cfg.node_id,
        cfg.log_level,
        cfg.asd_id,
        cfg.__sync_dont_use,
        cfg.limit,
        cfg.multicast,
        cfg.buffer_size
      in

      (if not fsync
       then Lwt_log.warning "Fsync has been disabled, data will not be stored durably!!"
       else Lwt.return ()) >>= fun () ->

      verify_log_level log_level;
      Lwt_log.add_rule "*" (to_level log_level);

      let _ : Lwt_unix.signal_handler_id =
        Lwt_unix.on_signal Sys.sigusr1 (fun _ ->
            let handle () =
              Lwt_log.info_f "Received USR1, reloading log level from config file" >>= fun () ->
              read_cfg () >>= function
              | `Error err ->
                Lwt_log.info_f "Not reloading config as it could not be parsed"
              | `Ok cfg ->
                let log_level = cfg.Config.log_level in
                Lwt_log.reset_rules ();
                Lwt_log.add_rule "*" (to_level log_level);
                Lwt_log.info_f "Changed log level to %s" log_level
            in
            Lwt.ignore_result (Lwt_extra2.ignore_errors handle)) in

      Asd_server.run_server ips port home ~asd_id ~node_id ~slow
                            ~fsync ~limit ~multicast ~buffer_size
  in

  lwt_server t

let asd_start_cmd =
  let cfg_file =
    Arg.(required
         & opt (some file) None
         & info ["config"] ~docv:"CONFIG_FILE" ~doc:"asd config file")
  in
  let asd_start_t =
    Term.(pure asd_start
          $ cfg_file
          $ Arg.(value
                 & flag
                 & info ["slow"] ~docv:"artifically slow down an asd (only for testing purposes!)"))
  in
  let info =
    Term.info
      "asd-start"
      ~doc:"start ASD server"
  in
  asd_start_t, info

let run_with_asd_client' host port asd_id f =
    lwt_cmd_line false (fun () -> Asd_client.with_client host port asd_id f)

let run_with_osd_client'
      (ips:string list)
      (port:int) osd_id f =
    lwt_cmd_line
      false
      (fun () ->
       Discovery.get_kind ips port >>= function
       | None -> failwith "what kind is this?"
       | Some k ->
          Remotes.Pool.Osd.factory k >>= fun (client, closer) ->
          Lwt.finalize (fun () ->f client) closer
      )

let asd_set host port asd_id key value =
  run_with_asd_client'
    host port asd_id
    (fun client ->
     let checksum = Checksum.NoChecksum in
     Lwt_log.warning "checksum option will be `NoChecksum`"
     >>= fun ()->
     client # apply_sequence []
             [ Update.set
                 (Slice.wrap_string key)
                 (Slice.wrap_string value)
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
                        $ hosts $ (port 8_000) $ lido
                        $ key $ value) in
  let info =
    let doc = "perform a set on a remote ASD" in
    Term.info "asd-set" ~doc in asd_set_t, info

let asd_get_version host port asd_id =
  run_with_asd_client'
    host port asd_id
    (fun client ->
     client # get_version () >>= fun (major,minor,patch, hash) ->
     Lwt_io.printlf "(%i, %i, %i, %S)" major minor patch hash
    )

let asd_get_version_cmd =
  let asd_get_version_t =
    Term.(pure asd_get_version
          $ hosts $ (port 8_000) $ lido
    ) in
  let info = Term.info "asd-get-version"
                       ~doc:"get remote version"
  in
  asd_get_version_t, info

let asd_multi_get host port asd_id (keys:string list) =
  run_with_asd_client'
    host port asd_id
    (fun client ->

     client # multi_get (List.map Slice.wrap_string keys)
     >>= fun values ->
     print_endline ([%show: (Slice.t * Checksum.t) option list] values);
     Lwt.return ())

let asd_multi_get_cmd =
  let doc = "$(docv)" in
  let keys = Arg.(non_empty &
                 pos_all string [] &
                 info [] ~docv:"KEYS" ~doc) in
  let asd_multi_get_t = Term.(pure asd_multi_get
                              $ hosts $ (port 8_000) $ lido
                              $ keys) in
  let info =
    let doc = "perform a multi get on a remote ASD" in
    Term.info "asd-multi-get" ~doc
  in asd_multi_get_t, info


let asd_delete host port asd_id key =
  run_with_asd_client'
    host port asd_id
    (fun client -> client # delete (Slice.wrap_string key))

let asd_delete_cmd =
  let doc = "$(docv)" in
  let key = Arg.(required &
                 pos 0 (some string) None &
                 info [] ~docv:"KEY" ~doc) in
  let asd_delete_t = Term.(pure asd_delete
                           $ hosts $ (port 8_000) $ lido
                           $ key)
  in
  let info =
    let doc = "perform a delete on a remote ASD" in
    Term.info "asd-delete" ~doc
  in asd_delete_t, info


let asd_range host port asd_id first =
  let finc = true
  and last = None in
  run_with_asd_client'
    host port asd_id
    (fun client ->
     client # range
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
                          $ hosts $ (port 8_000) $ lido
                          $ first) in
  let info =
    let doc = "range query on a remote ASD" in
    Term.info "asd-range" ~doc
  in
  asd_range_t, info

let osd_bench host port osd_id n value_size power prefix =
  run_with_osd_client'
    host port osd_id
    (fun client ->
     Osd_bench.do_all
       client n value_size power prefix
    )

let osd_bench_cmd =
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
  let osd_bench_t = Term.(pure osd_bench
                          $ hosts $ port 10000 $ lido
                          $ n 10000
                          $ value_size 16384
                          $ power 4
                          $ prefix ""
                    )
  in
  let info =
    Term.info
      "osd-benchmark"
      ~doc:"perform a benchmark on an osd"
  in
  osd_bench_t, info

let asd_statistics hosts port_o asd_id to_json config_o clear =
  let _inner =
    (fun (client:Asd_client.client) ->
     client # statistics clear >>= fun stats ->
     if to_json
     then
       let open Alba_json.AsdStatistics in
       print_result (make stats) to_yojson
     else Lwt_io.printl ([%show: Asd_statistics.AsdStatistics.t] stats)
    )
  in
  let from_asd hosts port asd_id =
    begin
       run_with_asd_client'
         hosts port asd_id
         _inner
    end
  in
  match port_o, config_o with
  | None, None -> failwith "specify either host & port or config"
  | None, Some cfg_file ->
     begin
       let t () =
         with_alba_client
           cfg_file
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
                  let ips,port =
                    let open Albamgr_protocol.Protocol.Osd in
                    get_ips_port osd.kind
                  in
                  Asd_client.with_client
                    ips port asd_id
                    _inner
              end
           )
       in
       lwt_cmd_line to_json t
     end
  | Some port,_ -> from_asd hosts port asd_id

let asd_statistics_cmd =
  let doc = "$(docv)" in
  let port_o =
    Arg.(value
         & opt (some int) None
         & info ["p";"port"] ~docv:"PORT" ~doc:"port"
    )
  in
  let config_o =
    Arg.(value
         & opt (some non_dir_file) None
         & info ["config"] ~docv:"CONFIG" ~doc:"the alba mgr config file (arakoon cfg)"
    )
  in

  let t = Term.(pure asd_statistics
                $ hosts
                $ port_o
                $ lido
                $ to_json
                $ config_o
                $ clear
          )
  in
  let info = Term.info "asd-statistics" ~doc in
  t, info


let asd_discover () =
  let open Discovery in
  let seen = function
    | Bad s -> Lwt_io.printlf "bad json:\n%S" s
    | Good (_, record) ->
       let extras_s = [%show : extra_info option ]
                       record.extras
       in
       Lwt_io.printlf
         "{extras: %s ; ips = %s; port = %i}%!"
         extras_s
         ([%show: string list] record.ips)
         record.port
  in
  lwt_cmd_line false (fun () -> discovery seen)

let asd_discover_cmd =
  let term = Term.(pure asd_discover $  pure ()) in
  let info = Term.info "asd-discovery" ~doc:"listen for ASD servers" in
  term,info


let asd_set_full host port asd_id full =
  run_with_asd_client'
    host port asd_id
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
          $ hosts $ (port 8_000) $ lido
          $ full
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
