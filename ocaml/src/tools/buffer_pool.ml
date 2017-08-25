(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

type t = Lwt_bytes.t Weak_pool.t

let create ~buffer_size =
  let factory () = Lwt_bytes.create ~msg:"buffer_pool" buffer_size in
  Weak_pool.create factory

let get_buffer = Weak_pool.take

let return_buffer = Weak_pool.return

let with_buffer t f =
  let buffer = get_buffer t in
  Lwt.finalize
    (fun () -> f buffer)
    (fun () -> return_buffer t buffer;
               Lwt.return ())

let pool_4k = create ~buffer_size:4096
let pool_8k = create ~buffer_size:8192
let pool_16k = create ~buffer_size:16384
let pool_32k = create ~buffer_size:32768

let default_buffer_pool = pool_4k
let osd_buffer_pool = create ~buffer_size:(768*1024)
