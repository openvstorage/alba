(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Cmdliner
open Lwt.Infix

let stderr_logger =
  let lg_output = Lwt_io.eprintl in
  let close = Lwt.return in
  lg_output, close

let log_sinks =
  Arg.(value
       & opt_all string [ "console:" ]
       & info ["log-sink"]
              ~doc:"specify log output sinks (e.g. console:, /path/to/file or redis://localhost[:port]/key")

let install_logger ?(log_sinks=`Stdout) ~subcomponent ~verbose () =
  let level =
    if verbose
    then Lwt_log.Debug
    else Lwt_log.Info
  in
  Lwt_log.append_rule "*" level;

  begin
    match log_sinks with
    | `Arakoon_sinks log_sinks ->
       Lwt_list.map_p
         (fun log_sink ->
          let open Arakoon_log_sink in
          match make log_sink with
          | File file_name ->
             Arakoon_logger.file_logger file_name
          | Redis (host, port, key) ->
             Arakoon_redis.make_redis_logger
               ~host ~port ~key
             |> Lwt.return
          | Console ->
             Lwt.return Arakoon_logger.console_logger)
         log_sinks
    | `Stdout ->
       Lwt.return [ Arakoon_logger.console_logger ]
    | `Stderr ->
       Lwt.return [ stderr_logger ]
  end >>= fun loggers ->

  let hostname = Unix.gethostname () in
  let component = Printf.sprintf "alba/%s" subcomponent in
  let pid = Unix.getpid () in
  let seqnum = ref 0 in
  let thread_id = 0 in
  let logger = Lwt_log.make
                 ~output:(fun section level lines ->
                          let seqnum' = !seqnum in
                          let () = incr seqnum in
                          let ts = Core.Time.now () in
                          let logline =
                            Arakoon_logger.format_message
                              ~hostname ~pid ~thread_id ~component
                              ~ts ~seqnum:seqnum'
                              ~section ~level ~lines
                          in
                          Lwt_list.iter_p
                            (fun (lg_output, _) -> lg_output logline)
                            loggers)
                 ~close:(fun () ->
                         Lwt_list.iter_p
                           (fun (_, close) -> close ())
                           loggers)
  in
  Lwt_log.default := logger;
  Lwt.return ()

let print_result result tojson =
  let open Alba_json in
  let json =
    Result.to_yojson
      tojson
      Result.({
          success = true;
          result;
        })
  in
  Lwt_io.printl (Yojson.Safe.pretty_to_string json)

let exn_to_string_code = function
  | Nsm_model.Err.Nsm_exn (e, _) ->
    let open Nsm_model.Err in
    "nsm_exn", err2int e,
    Printf.sprintf "Namespace manager exception: %s" (show e)
  | Albamgr_protocol.Protocol.Error.Albamgr_exn (e, msg) ->
    let open Albamgr_protocol.Protocol.Error in
    "albamgr_exn", err2int e,
    Printf.sprintf "Albamgr exception(%s,%s)" (show e) msg
  | Proxy_protocol.Protocol.Error.Exn (e, _) ->
    let open Proxy_protocol.Protocol.Error in
    "proxy_exn", err2int e,
    Printf.sprintf "Proxy exception: %s" (show e)
  | Asd_protocol.Protocol.Error.Exn e ->
    let open Asd_protocol.Protocol.Error in
    "asd_exn", get_code e,
    Printf.sprintf "Asd exception: %s" (show e)
  | Alba_client_errors.Error.Exn e ->
     let open Alba_client_errors.Error in
     "client_exn", to_enum e,
     Printf.sprintf "Client_exception: %s" (show e)
  | exn ->
    "unknown", 0,
    Printexc.to_string exn

let lwt_cmd_line ~to_json ~verbose t =
  let t' () =
    Lwt.catch
      (fun () ->
        install_logger ~log_sinks:`Stderr ~subcomponent:"cli" ~verbose ()
        >>= fun () ->
        t () >>= fun () ->
        Lwt.return 0)
      (fun exn ->
        let exc_type, exc_code, message = exn_to_string_code exn in
        begin
          if to_json
          then
            Lwt_io.printlf
              "%s"
              (Yojson.Safe.to_string
                 (`Assoc [
                     ("success", `Bool false);
                     ("error", `Assoc [
                                  ("message", `String message);
                                  ("exception_type", `String exc_type);
                                  ("exception_code", `Int exc_code);
                     ])
              ])) >>= fun () ->
            Lwt.return 0
          else
            Lwt_log.warning message >>= fun () ->
            Lwt.return 2
        end

      )
  in
  let rc = Lwt_main.run (t' ()) in
  exit rc

let unit_result to_json () =
  if to_json
  then print_result () (fun () -> `Assoc [])
  else Lwt.return_unit

let version_result to_json version =
  if to_json
  then
    begin
      print_result version Alba_json.Version.to_yojson
    end
  else
    let major, minor, patch, hash = version in
    Lwt_io.printlf "(%i, %i, %i, %S)" major minor patch hash

let lwt_cmd_line_result ~to_json ~verbose t res_to_json =
  lwt_cmd_line
    ~to_json ~verbose
    (fun () ->
       t () >>= fun res ->
       if to_json
       then print_result res res_to_json
       else Lwt.return ())

let lwt_cmd_line_unit ~to_json ~verbose t =
  lwt_cmd_line_result
    ~to_json ~verbose
    t
    (fun () -> `Assoc [])

let lwt_server ~log_sinks ~subcomponent t : unit =
  let () = Sys.set_signal Sys.sigpipe Sys.Signal_ignore in
  Lwt_main.run
    begin
      install_logger
        ~log_sinks:(`Arakoon_sinks log_sinks)
        ~subcomponent
        ~verbose:false () >>= fun () ->
      t ()
    end

let url_converter :Prelude.Url.t Arg.converter =
  let url_parser s =
    try
      `Ok (Url.make s)
    with exn ->
      `Error (Printexc.to_string exn)
  in
  let url_printer fmt s= Format.pp_print_string fmt (Url.show s)
  in
  url_parser, url_printer

let alba_cfg_url =
  let doc = "config url for the alba mgr (fe file:///.... or etcd://127.0.0.1:5000/...)" in
  let env = Arg.env_var "ALBA_CONFIG" ~doc in
  let docv = "<abm config url>" in
  Arg.(required
       & opt (some url_converter) None
       & info ["config"]
              ~env ~docv ~doc )



let to_json =
  Arg.(value
       & flag
       & info ["to-json"] ~doc:"only output json to stdout")

let verbose =
  Arg.(value
       & flag
       & info ["verbose"] ~doc:"more output on cli")

let unescape =
  Arg.(value
       & flag & info ["unescape"] ~doc:"ocaml unescape keys"
  )

let maybe_unescape unescape s =
  if unescape
  then Scanf.unescaped s
  else s

let port default =
  let doc = "tcp $(docv)" in
  Arg.(value
       & opt int default
       & info ["p"; "port"] ~docv:"PORT" ~doc)

let port_option =
  Arg.(value
       & opt (some int) None
       & info ["port"; "p";]
              ~docv:"PORT"
              ~doc:"the port to connect with")

let attempts default =
  let doc = "number of attempts" in
  Arg.(value
       & opt int default
       & info ["attempts"] ~docv:"ATTEMPTS" ~doc
  )

let host =
  Arg.(value
       & opt string "::1"
       & info ["h";"host"] ~docv:"HOST" ~doc:"the host to connect with")

let hosts =
  let doc = "listen on $(docv)" in
  Arg.(value
       & opt_all string []
       & info ["h";"host"] ~docv:"HOST" ~doc)

let host_option =
  Arg.(value
       & opt (some string) None
       & info ["host"; "h";]
              ~docv:"HOST"
              ~doc:"the host to connect with")

let transport =
  let (tr : Net_fd.transport Arg.converter) =
    let parser x = match String.lowercase_ascii x with
      | "tcp"  -> `Ok Net_fd.TCP
      | "rdma" -> `Ok Net_fd.RDMA
      | x      ->
         let msg =
           Printf.sprintf "%S is not a transport. Specify either \"tcp\" or \"rdma\"." x
         in
         `Error msg
    in
    let printer fmt transport =
      Format.pp_print_string fmt (Net_fd.show_transport transport)
    in
    parser, printer
  in
  Arg.(value
       & opt tr Net_fd.TCP
       & info ["t";"transport"]
              ~docv:"TRANSPORT"
              ~doc:"either `tcp` or `rdma`"
  )

let namespace p =
  let doc = "namespace" in
  Arg.(required
       & pos p (some string) None
       & info [] ~docv:"NAMESPACE" ~doc)

let nsm_host p =
  Arg.(required
       & pos p (some string) None
       & info [] ~docv:"NSM_HOST" ~doc:"nsm host")

let nsm_hosts =
  Arg.(value
       & opt (list string) []
       & info [ "nsm-hosts"; ]
              ~docv:"NSM_HOSTs"
              ~doc:"comma separated list of $(docv)")

let preset_name_namespace_creation p =
  Arg.(value
       & pos p (some string) None
       & info
         []
         ~docv:"PRESET_NAME"
         ~doc:"name of the preset to be used when creating the new namespace")

let long_id =
  let doc = "$(docv) of the osd to connect with" in
  Arg.(required
       & opt (some string) None
       & info ["long-id"] ~docv:"LONG_ID" ~doc
  )

let lido =
  let doc = "option $(docv) of the OSD to connect with" in
  Arg.(value
       & opt (some string) None
       & info ["long-id"] ~docv:"LONG_ID" ~doc
  )

let long_ids =
  let doc = "comma separated list of $(docv)" in
  Arg.(value
       & opt (list string) []
       & info ["long-id"] ~docv:"LONG_IDs" ~doc
  )

let consistent_read =
  Arg.(value
       & flag
       & info ["consistent-read"]
              ~docv:"CONSISTENT_READ"
              ~doc:"specify whether the read should be consistent"
  )

let clear =
  Arg.(value
       & flag
       & info ["clear"] ~doc:"clear immediately after returning the stats"
  )

let file_upload p =
  Arg.(required
       & pos p (some non_dir_file) None
       & info [] ~docv:"FILE" ~doc:"file to upload")

let file_download p =
  Arg.(required &
       pos p (some string) None &
       info [] ~docv:"FILE" ~doc:"destination file to write the object to")

let object_name_upload p =
  Arg.(required
       & pos p (some string) None
       & info []
         ~docv:"OBJECT_NAME"
         ~doc:"the name for the object in Alba"
      )

let object_name_download p =
  Arg.(required &
       pos p (some string) None &
       info [] ~docv:"OBJECT_NAME" ~doc:"the object to download from alba")

let allow_overwrite =
  Arg.(value
       & flag
       & info ["allow-overwrite"]
         ~docv:"ALLOW-OVERWRITE"
         ~doc:"flag to allow overwriting the object if it already exists")

let first =
  Arg.(value & opt string ""
       & info["first"]
             ~doc:"starting key")


let finc =
  Arg.(value & opt bool true & info ["finc"] ~doc:"")


let last =
  Arg.(value & opt (some string) None & info["last"] ~doc:"last key")


let max =
  Arg.(value & opt int 100 & info["max"] ~doc:"")

let reverse =
  Arg.(value & opt bool false & info["reverse"] ~doc:"")

let tls_config =
  let open Arg in
  let (tls: Tls.t Arg.converter) =
    let pa,pr = (t3 string string string) in
    let parser  =
      begin
        fun s ->
        match pa s with
        | `Ok cck -> `Ok (Tls.make cck)
        | `Error x -> `Error x
      end
    and printer = (fun fmt tls -> Format.pp_print_string fmt (Tls.show tls))
    in (parser, printer)

  in
  let doc = "<cacert.pem,mycert.pem,mykey.key>" in
  let env = Arg.env_var "ALBA_CLI_TLS" ~doc in
  Arg.(value
       & opt (some tls) None
       & info ["tls"] ~env ~doc
  )

let produce_xml default =
  let doc = "produce xml in ./testresults.xml. $(docv): bool" in
  Arg.(value
       & opt bool default
       & info ["xml"] ~docv:"XML" ~doc)


let only_test =
  Arg.(value
       & opt_all string []
       & info ["only-test"] ~docv:"ONLY-TEST" ~doc:"limit tests to filter:$(docv)"
  )

let node_id default =
  let doc = "the $(docv) just for this node" in
  Arg.(value
       & opt (some string) default
       & info ["node-id"] ~docv:"NODE_ID" ~doc
      )

let verify_log_level log_level =
  let levels = [ "debug"; "info"; "notice"; "warning"; "error"; "fatal"; ] in
  if not (List.mem log_level levels)
  then failwith (Printf.sprintf
                   "log_level: got %s but should be one of %s"
                   log_level
                   ([%show : string list] levels))
let to_level =
  let open Lwt_log in
  function
  | "debug" -> Debug
  | "info" -> Info
  | "notice" -> Notice
  | "warning" -> Warning
  | "error" -> Error
  | "fatal" -> Fatal
  | log_level -> failwith (Printf.sprintf "unknown log level %s" log_level)

let with_alba_client ?albamgr_connection_pool_size cfg_url tls_config f =
  Alba_arakoon.config_from_url cfg_url >>= fun cfg ->
  let cfg_ref = ref cfg in
  Alba_client2.with_client
    cfg_ref
    ~tls_config
    ~populate_osds_info_cache:false
    ~upload_slack:0.2
    ?albamgr_connection_pool_size
    f

let with_albamgr_client ~attempts cfg_url tls_config f =
  Alba_arakoon.config_from_url cfg_url >>= fun cfg ->
  Albamgr_client.with_client' ~attempts
    cfg ~tls_config f
