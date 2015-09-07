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
