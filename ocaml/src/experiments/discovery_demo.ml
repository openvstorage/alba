(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

let () =
  let open Discovery in
  let seen = function
    | Bad s -> Lwt_io.printlf "invalid json:\n%S%!" s
    | Good (json,{extras;ips;port;}) ->
       let extras_s = [%show : extra_info option] extras in
       Lwt_io.printlf
         "seen: {extras=%s; ips=[%s];port=%i }%!"
         extras_s
         (String.concat ";" ips)
         port
  in
  let t =

    match Sys.argv.(1) with
    | "send"     ->
       let home = "/"
       and ips = ["10.100.0.128";"127.0.0.1"]
       and node_id = "the node!"
       and port = 8000
       and period = 10.0
       in
       broadcast home "demo" node_id ips port period
    | "discover" -> discovery seen
    | "get_kind" ->
       begin
         let open Lwt.Infix in
         let ips = [Sys.argv.(2)] in
         let port = Scanf.sscanf Sys.argv.(3) "%i" (fun i -> i) in
         get_kind ips port >>= function
         | None -> Lwt_io.eprintlf "dunno..."
         | Some x -> Lwt_io.printlf
                       "%s"
                       ([%show : Albamgr_protocol.Protocol.Osd.kind] x)
       end
    | _ -> failwith "send | discover | get_kind"
  in
  let () = Cli_common.install_logger () in
  Lwt_main.run t
