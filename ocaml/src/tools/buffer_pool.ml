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

type t = Lwt_bytes.t Weak_pool.t

let create ~buffer_size =
  Weak_pool.create (fun () -> Lwt_bytes.create buffer_size)

let get_buffer = Weak_pool.take

let return_buffer = Weak_pool.return

let with_buffer t f =
  let buffer = get_buffer t in
  Lwt.finalize
    (fun () -> f buffer)
    (fun () -> return_buffer t buffer;
               Lwt.return ())

let default_buffer_pool = create ~buffer_size:4096
let osd_buffer_pool = create ~buffer_size:(768*1024)
