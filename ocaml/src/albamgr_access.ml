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

open Remotes

class basic_mgr_pooled (albamgr_pool : Pool.Albamgr.t) =
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
