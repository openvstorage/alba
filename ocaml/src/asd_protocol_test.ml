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
