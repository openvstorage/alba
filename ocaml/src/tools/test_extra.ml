(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)


open! Prelude

let lwt_run t =
  let nfds0 = Alba_wrappers.Sys2.get_num_fds () in
  let rc = Lwt_main.run t in
  let nfds = Alba_wrappers.Sys2.get_num_fds () in
  let () = Printf.printf "n_fds: before:%i after:%i\n%!" nfds0 nfds in
  rc
