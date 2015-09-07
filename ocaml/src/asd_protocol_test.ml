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
