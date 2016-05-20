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

open OUnit
open Asd_config
let test_basic_config () =
  let cfg =
    let open Config in
    {
      ips = [];
      port = None;
      transport = "tcp";
      node_id = "the_node_id";
      home = "/home/";
      log_level = "debug";
      asd_id = None;
      __sync_dont_use = false;
      limit = 99L;
      capacity = None;
      buffer_size = None;
      multicast = Some 10.0;
      tls = None;
      tcp_keepalive = Tcp_keepalive2.default;
      __warranty_void__write_blobs = true;
      use_fadvise = true;
      use_fallocate = true;
      rocksdb_block_cache_size = None;
      blob_io_engine = GioExecFile "the_config_file";
    }
  in
  Printf.printf "cfg=%s\n%!" (Asd_config.Config.show cfg);
  let json = Asd_config.Config.to_yojson cfg in
  let value = Yojson.Safe.pretty_to_string json in
  Printf.printf "json=%s\n%!" value;
  ()

let suite =
  "asd_config_test" >:::[
      "test_basic_config" >:: test_basic_config
    ]
