(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
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
