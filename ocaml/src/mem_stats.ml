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

let reporting_t ~section ?(f=Lwt.return) () =
  let factor = (float (Sys.word_size / 8)) /. 1024.0 in
  let rec loop () =
    Lwt_unix.sleep 60.0 >>= fun () ->
    Alba_wrappers.Sys2.lwt_get_maxrss () >>= fun maxrss ->
    let stat = Gc.quick_stat () in
    let mem_allocated = (stat.Gc.minor_words +. stat.Gc.major_words -. stat.Gc.promoted_words)
                        *. factor in

    Lwt_log.info_f ~section
                   "maxrss:%i KB, allocated:%f, minor_collections:%i, major_collections:%i, compactions:%i, heap_words:%i"
                   maxrss mem_allocated stat.Gc.minor_collections
                   stat.Gc.major_collections stat.Gc.compactions
                   stat.Gc.heap_words >>=
    f >>=
    loop
  in
  loop ()
