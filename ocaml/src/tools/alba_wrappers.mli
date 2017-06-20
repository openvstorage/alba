(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

module Sys2 : sig
    val get_maxrss : unit -> int
    val lwt_get_maxrss : unit -> int Lwt.t
    val get_num_fds : unit -> int
end
