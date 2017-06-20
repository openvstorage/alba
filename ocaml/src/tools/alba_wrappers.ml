(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

external alba_get_maxrss : unit -> int = "alba_get_maxrss"
external alba_get_num_fds : unit -> int = "alba_get_num_fds"

module Sys2 = struct
    let get_maxrss () = alba_get_maxrss ()
    let lwt_get_maxrss () = Lwt_preemptive.detach get_maxrss ()
    let get_num_fds ()= alba_get_num_fds()
end
