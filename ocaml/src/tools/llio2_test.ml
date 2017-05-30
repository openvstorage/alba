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
