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

open Remotes

class basic_mgr_pooled
        buffer_pool
        ~albamgr_connection_pool_size
        ~albamgr_client_cfg
        ~tls_config
  =

  let albamgr_pool =
    Pool.Albamgr.make
      ~size:albamgr_connection_pool_size
      albamgr_client_cfg tls_config
      buffer_pool
  in
  let with_basic_albamgr_from_pool f =
    Pool.Albamgr.use_mgr
      albamgr_pool
      f
  in
  object(self :# Albamgr_client.basic_client)

    method query ?consistency command req =
      with_basic_albamgr_from_pool
        (fun mgr -> mgr # query ?consistency command req)

    method update command req =
      with_basic_albamgr_from_pool
        (fun mgr -> mgr # update command req)
  end
