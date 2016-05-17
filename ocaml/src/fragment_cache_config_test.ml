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

open Lwt.Infix
open Fragment_cache_config

type t_list = fragment_cache list [@@deriving yojson, show]

let test_example_config () =
  let caches = [
      None';
      Local {
          path = "/tmp/x";
          max_size = 100000;
          rocksdb_max_open_files = default_rocksdb_max_open_files;
          cache_on_read = true; cache_on_write = false;
        };
      Local {
          path = "/tmp/x";
          max_size = 100000;
          rocksdb_max_open_files = 256;
          cache_on_read = true; cache_on_write = false;
        };
      Alba {
          albamgr_cfg_url = "/tmp/x";
          bucket_strategy = Fragment_cache_alba.(OneOnOne { preset = "mypreset";
                                                            prefix = "myprefix"; });
          fragment_cache = None';
          manifest_cache_size = 5_000_000;
          albamgr_connection_pool_size  = default_connection_pool_size;
          nsm_host_connection_pool_size = default_connection_pool_size;
          osd_connection_pool_size      = default_connection_pool_size;
          osd_timeout = default_osd_timeout;
          tls_client = Some Tls.({ ca_cert = "/tmp/cacert.pem";
                                   creds = Some ("/tmp/my_client/my_client.pem",
                                                 "/tmp/my_client/my_client.key") });
          cache_on_read_ = true; cache_on_write_ = false;
        };
      Alba {
          albamgr_cfg_url = "/tmp/x";
          bucket_strategy = Fragment_cache_alba.(OneOnOne { preset = "mypreset";
                                                            prefix = "myprefix"; });
          fragment_cache = Local {
                               path = "/tmp/x";
                               max_size = 100_000;
                               rocksdb_max_open_files = default_rocksdb_max_open_files;
                               cache_on_read = true; cache_on_write = false;
                             };
          manifest_cache_size = 5_000_000;
          albamgr_connection_pool_size  = default_connection_pool_size;
          nsm_host_connection_pool_size = default_connection_pool_size;
          osd_connection_pool_size      = default_connection_pool_size;
          osd_timeout = default_osd_timeout;
          tls_client = None;
          cache_on_read_ = true; cache_on_write_ = false;
        };
    ]
  in
  let file = "./cfg/fragment_cache_config.json" in
  let t () =
    Lwt_extra2.read_file file >>= fun txt ->
    let json = Yojson.Safe.from_string txt in
    let caches' = match t_list_of_yojson json with
      | `Error e -> failwith e
      | `Ok r -> r
    in
    Lwt_log.debug_f "Expected %s, got %s"
                    (show_t_list caches)
                    (show_t_list caches') >>= fun () ->
    assert (caches = caches');
    Lwt.return ()
  in
  Lwt_main.run (t ())

open OUnit

let suite = "fragment_cache_config_test" >:::[
      "test_example_config" >:: test_example_config;
    ]
