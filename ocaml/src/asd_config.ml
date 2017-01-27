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

module Config = struct

  type tls = { cert: string;
               key : string;
               port: int;
             } [@@deriving yojson, show]

  type t = {
    ips : (string list         [@default []]);
    port :(int option          [@default None]);
    transport : (string        [@default "tcp"]);

    rora_ips : (string list option [@default None]);
    rora_port : (int option [@default None]);
    rora_transport : (string option [@default None]);
    rora_num_cores : (int option [@default None]);
    rora_queue_depth : (int option [@default None]);

    node_id : string;
    home : string;
    log_level : string;
    asd_id : (string option    [@default None]);
    __sync_dont_use : (bool    [@default true]);

    limit : (int64             [@default 99L]);
    capacity : (int64 option [@default None]);

    buffer_size : (int option  [@default None]); (* deprecated *)

    multicast: (float option   [@default (Some 10.0)]);
    tls : (tls option          [@default None]);
    tcp_keepalive : (Tcp_keepalive2.t [@default Tcp_keepalive2.default]);
    __warranty_void__write_blobs  : (bool [@default true]);

    use_fadvise  : (bool [@default true]);
    use_fallocate: (bool [@default true]);

    rocksdb_block_cache_size : (int option [@default None]);
    lwt_unix_pool_size : (int [@default 100]);
  } [@@deriving yojson, show]
end


let retrieve_cfg_from_string cfg_string =
  let () = Lwt_log.ign_info_f "Found the following config: %s" cfg_string  in
  let config = Config.of_yojson (Yojson.Safe.from_string cfg_string) in

  (match config with
   | Result.Error err ->
      Lwt_log.ign_warning_f "Error while parsing cfg file: %s" err
   | Result.Ok cfg ->
      Lwt_log.ign_info_f
        "Interpreted the config as: %s"
        ([%show : Config.t] cfg))
  ;
  config

let retrieve_cfg cfg_url =
  let open Lwt.Infix in
  Arakoon_config_url.retrieve cfg_url >|= retrieve_cfg_from_string
