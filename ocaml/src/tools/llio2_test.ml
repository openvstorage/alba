(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Llio2

let test_list () =
  let tests = [ [1;2;3;4;5] ] in
  List.iter
    (fun t ->
      let wbuf = WriteBuffer.make ~length:100 in
      let () = WriteBuffer.list_to WriteBuffer.int_to wbuf t in
      let open WriteBuffer in
      let rbuf = ReadBuffer.make_buffer wbuf.buf
                                        ~offset:0 ~length:wbuf.pos
      in
      let t2 = ReadBuffer.list_from ReadBuffer.int_from rbuf in
      let () = dispose wbuf in
      let printer = [%show : int list] in
      OUnit.assert_equal ~printer t t2;
    )
  tests


open OUnit

let suite = "llio2" >::: [
   "test_list" >:: test_list
    ]
