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

open! Prelude

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

let pool_4k = create ~buffer_size:4096
let pool_8k = create ~buffer_size:8192
let pool_16k = create ~buffer_size:16384
let pool_32k = create ~buffer_size:32768

let default_buffer_pool = pool_4k
let osd_buffer_pool = create ~buffer_size:(768*1024)
