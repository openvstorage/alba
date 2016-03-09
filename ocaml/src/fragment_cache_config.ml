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
open Prelude

let default_rocksdb_max_open_files = 256
let default_connection_pool_size = 10
let default_osd_timeout = 2.

type fragment_cache =
  | None' [@name "none"]
  | Local of local_fragment_cache [@name "local"]
  | Alba of alba_fragment_cache [@name "alba"]
  [@@deriving yojson, show]

and local_fragment_cache = {
    path : string;
    max_size : int;
    rocksdb_max_open_files : (int [@default default_rocksdb_max_open_files]);
  } [@@deriving yojson, show]

and alba_fragment_cache = {
    albamgr_cfg_url : string;
    bucket_strategy : Fragment_cache_alba.alba_fragment_cache_bucket_strategy;
    fragment_cache : (fragment_cache [@default None']);
    manifest_cache_size : int;
    albamgr_connection_pool_size  : (int [@default default_connection_pool_size]);
    nsm_host_connection_pool_size : (int [@default default_connection_pool_size]);
    osd_connection_pool_size      : (int [@default default_connection_pool_size]);
    osd_timeout : float [@default default_osd_timeout];
    tls_client : Tls.t option [@default None];
  } [@@deriving yojson, show]

let rec make_fragment_cache = function
  | None' ->
     Lwt.return (new Fragment_cache.no_cache :> Fragment_cache.cache)
  | Local { path; max_size; rocksdb_max_open_files; } ->
     assert (path.[0] = '/');
     let max_size = Int64.of_int max_size in
     let max_size =
       (* the fragment cache size is currently a rather soft limit which we'll
        * surely exceed. this can lead to disk full conditions. by taking a
        * safety margin of 15% we turn the soft limit into a hard one... *)
       let open Int64 in
       mul (div max_size 100L) 85L
     in
     Fragment_cache.safe_create path ~max_size ~rocksdb_max_open_files
     >>= fun cache ->

     let rec fragment_cache_disk_usage_t () =
       Lwt.catch
         (fun () ->
          Fsutil.lwt_disk_usage path >>= fun (used_b, total_b) ->
          let percentage =
            100.0 *. ((Int64.to_float used_b) /. (Int64.to_float total_b))
          in
          Lwt_log.info_f "fragment_cache disk_usage: %.2f%%" percentage)
         (fun exn -> Lwt_log.warning ~exn "fragment_cache_disk_usage_t")
       >>= fun () ->
       Lwt_unix.sleep 60.0 >>= fun () ->
       fragment_cache_disk_usage_t ()
     in
     Lwt.ignore_result (fragment_cache_disk_usage_t ());

     Lwt.return (cache :> Fragment_cache.cache)
  | Alba { albamgr_cfg_url;
           bucket_strategy;
           fragment_cache;
           manifest_cache_size;
           albamgr_connection_pool_size;
           nsm_host_connection_pool_size;
           osd_connection_pool_size;
           osd_timeout;
           tls_client;
         } ->
     make_fragment_cache fragment_cache >>= fun nested_fragment_cache ->
     Alba_arakoon.config_from_url (Url.make albamgr_cfg_url) >>= fun albamgr_cfg ->
     let cache = new Fragment_cache_alba.alba_cache
                     ~albamgr_cfg_ref:(ref albamgr_cfg)
                     ~bucket_strategy
                     ~nested_fragment_cache
                     ~manifest_cache_size
                     ~albamgr_connection_pool_size
                     ~nsm_host_connection_pool_size
                     ~osd_connection_pool_size
                     ~osd_timeout
                     ~tls_config:tls_client
     in
     Lwt.return (cache :> Fragment_cache.cache)
