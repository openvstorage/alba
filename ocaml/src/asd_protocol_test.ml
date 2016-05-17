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

open Asd_protocol
open Protocol
let roundtrip test_ctx =
  let k = "the_key" in
  let v = "the_value" in
  let ks = Slice.wrap_string k in
  let vs = Slice.wrap_string v in
  let update = ASDUpdate.AssertValue(ks,
                                     Some (vs))
  in
  let buf = Buffer.create 128 in
  let () = ASDUpdate.to_buf buf update in
  let buf_s =Buffer.contents buf in
  let () = Printf.printf "buf:%s\n" (Prelude.to_hex buf_s) in

  let rbuf = Llio.make_buffer buf_s 0 in
  let update' = ASDUpdate.from_buf rbuf in
  Printf.printf "update': %s\n" ([%show : ASDUpdate.t] update');
  ()


let () = roundtrip()
