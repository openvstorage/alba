(*
Copyright 2016 iNuron NV

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
        };
      Local {
          path = "/tmp/x";
          max_size = 100000;
          rocksdb_max_open_files = 256;
        };
      Alba {
          albamgr_cfg_url = "/tmp/x";
          namespaces = [ "a"; "b"; ];
          fragment_cache = None';
        };
      Alba {
          albamgr_cfg_url = "/tmp/x";
          namespaces = [ "a"; "b"; ];
          fragment_cache = Local {
                               path = "/tmp/x";
                               max_size = 100_000;
                               rocksdb_max_open_files = default_rocksdb_max_open_files;
                             };
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
