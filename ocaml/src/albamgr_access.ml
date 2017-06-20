(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
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

    method finalize =
      Lwt_pool2.finalize albamgr_pool |> Lwt.ignore_result
  end

class mgr_access (mgr : basic_mgr_pooled) =
object
  inherit Albamgr_client.client (mgr :> Albamgr_client.basic_client)

  method finalize = mgr # finalize
end

let make albamgr_pool =
  let basic_mgr_pooled = new basic_mgr_pooled albamgr_pool in
  let mgr_access = new mgr_access basic_mgr_pooled in
  mgr_access
