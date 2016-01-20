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

module Config = struct

  type tls = { cert: string;
               key : string;
               port: int;
             } [@@deriving yojson, show]

  type t = {
    ips : (string list         [@default []]);
    port :(int option          [@default None]);
    node_id : string;
    home : string;
    log_level : string;
    asd_id : (string option    [@default None]);
    __sync_dont_use : (bool    [@default true]);
    limit : (int64             [@default 99L]);
    buffer_size : (int         [@default (768*1024)]);
    multicast: (float option   [@default (Some 10.0)]);
    tls : (tls option          [@default None]);
    tcp_keepalive : (Tcp_keepalive2.t [@default Tcp_keepalive2.default]);
  } [@@deriving yojson, show]
end
open Lwt.Infix


let retrieve_cfg_from_string cfg_string =
  let config = Config.of_yojson (Yojson.Safe.from_string cfg_string) in
  (match config with
   | `Error err ->
      Lwt_log.warning_f "Error while parsing cfg file: %s" err
   | `Ok cfg ->
      Lwt_log.info_f
        "Interpreted the config as: %s"
        ([%show : Config.t] cfg)) >>= fun () ->
  Lwt.return config

let retrieve_cfg cfg_url =
  Prelude.Etcd.retrieve_cfg cfg_url retrieve_cfg_from_string
