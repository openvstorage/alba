(*
Copyright 2015 iNuron NV

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
let osd_buffer_pool = default_buffer_pool
