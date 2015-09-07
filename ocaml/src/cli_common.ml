(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

open Cmdliner
open Lwt.Infix

let install_logger ?(channel=Lwt_io.stdout) () =
  Lwt_log.append_rule "*" Lwt_log.Debug;
  let logger =
    Lwt_log.channel
      ~template:"$(date).$(milliseconds) $(section) $(level): $(message)"
      ~close_mode:`Keep
      ~channel ()
  in
  Lwt_log.default := logger

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
  Lwt_io.printlf
    "%s"
    (Yojson.Safe.to_string json)

let exn_to_string_code = function
  | Nsm_model.Err.Nsm_exn (e, _) ->
    let open Nsm_model.Err in
    "nsm_exn", err2int e,
    Printf.sprintf "Namespace manager exception: %s" (show e)
  | Albamgr_protocol.Protocol.Error.Albamgr_exn (e, _) ->
    let open Albamgr_protocol.Protocol.Error in
    "albamgr_exn", err2int e,
    Printf.sprintf "Albamgr exception: %s" (show e)
  | Proxy_protocol.Protocol.Error.Exn e ->
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

let lwt_cmd_line to_json t =
  let () = install_logger ~channel:Lwt_io.stderr () in
  let t' () =
    Lwt.catch
      t
      (fun exn ->
         begin
           let exc_type, exc_code, message = exn_to_string_code exn in
           if to_json
           then begin
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
                    ]))
           end else
             Lwt_log.warning message
         end >>= fun () ->

         Lwt.fail exn)
  in
  Lwt_main.run (t' ())

let lwt_cmd_line_result to_json t res_to_json =
  lwt_cmd_line
    to_json
    (fun () ->
       t () >>= fun res ->
       if to_json
       then print_result res res_to_json
       else Lwt.return ())

let lwt_cmd_line_unit to_json t =
  lwt_cmd_line_result
    to_json
    t
    (fun () -> `Assoc [])

let lwt_server t : unit =
  let () = install_logger () in
  let () = Sys.set_signal Sys.sigpipe Sys.Signal_ignore in
  Lwt_main.run (t ())

let alba_cfg_default_location =
  try Some (Sys.getenv "ALBA_CONFIG")
  with Not_found -> None

let alba_cfg_file =
  Arg.(required
       & opt (some file) alba_cfg_default_location
       & info ["config"] ~docv:"CONFIG_FILE" ~doc:"alba mgr (arakoon) config file")

let to_json =
  Arg.(value
       & flag
       & info ["to-json"] ~docv:"only output json to stdout")

let port default =
  let doc = "tcp $(docv)" in
  Arg.(value
       & opt int default
       & info ["p"; "port"] ~docv:"PORT" ~doc)

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

let namespace p =
  let doc = "namespace" in
  Arg.(required
       & pos p (some string) None
       & info [] ~docv:"NAMESPACE" ~doc)

let nsm_host p =
  Arg.(required
       & pos p (some string) None
       & info [] ~docv:"NSM_HOST" ~doc:"nsm host")

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
  Arg.(value & opt string "" & info["first"] ~doc:"")


let finc =
  Arg.(value & opt bool true & info ["finc"] ~doc:"")


let last =
  Arg.(value & opt (some string) None & info["last"] ~doc:"")


let max =
  Arg.(value & opt int 100 & info["max"] ~doc:"")

let reverse =
  Arg.(value & opt bool false & info["reverse"] ~doc:"")


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

let with_alba_client cfg_file f =
  Alba_client.with_client
    (ref (Albamgr_protocol.Protocol.Arakoon_config.from_config_file cfg_file))
    f

let with_albamgr_client ~attempts cfg_file f =
  let cfg =
    Albamgr_protocol.Protocol.Arakoon_config.from_config_file
      cfg_file
  in
  Albamgr_client.with_client' ~attempts
    cfg
    f
