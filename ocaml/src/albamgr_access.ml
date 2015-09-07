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

open Remotes

class basic_mgr_pooled
        buffer_pool
        ~albamgr_connection_pool_size
        ~albamgr_client_cfg =

  let albamgr_pool =
    Pool.Albamgr.make
      ~size:albamgr_connection_pool_size
      albamgr_client_cfg
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
