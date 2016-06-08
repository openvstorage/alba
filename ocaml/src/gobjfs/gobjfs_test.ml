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

open Gobjfs
open Lwt.Infix

let fill data=
  let len = Lwt_bytes.length data in
  let rec loop n =
    if n = len
    then ()
    else
      let c = Char.chr (n mod 256) in
      let () = Lwt_bytes.set data n c in
      loop (n+1)
  in
  loop 0

let check data off len =
  let rec loop n =
    if n = len
    then ()
    else
      let c = Lwt_bytes.get data n in
      let c' = Char.chr((off+n) mod 256) in
      let () = assert (c = c') in
      loop (n+1)
  in
  loop 0

let test_write event_fd fn =
  Lwt_io.printlf "test_write%!" >>= fun () ->
  let fragment_size = 8192 in
  let fragment= Fragment.make 42L 0 fragment_size in
  let data = Fragment.get_bytes fragment in
  fill data;
  let fragments = [ fragment ] in
  let batch = Batch.make fragments in
  IOExecFile.file_open fn [Unix.O_RDWR;Unix.O_CREAT] >>= fun handle ->
  IOExecFile.file_write handle batch >>= fun () ->
  Lwt_unix.wait_read event_fd >>= fun () ->
  let buf = Lwt_bytes.create 16 in
  IOExecFile.reap event_fd buf >>= fun (n, ss) ->
  assert (n = 1);
  let s0 = List.hd ss in
  Lwt_io.printlf "s0:%s" (IOExecFile.show_status s0) >>= fun () ->
  assert (IOExecFile.get_completion_id s0 = 42L);
  assert (IOExecFile.get_error_code s0 = 0l);
  Lwt_io.printlf "rest:%lx" (Ser.get32_prim' buf 0) >>= fun () ->
  IOExecFile.file_close handle

let test_read event_fd fn =
  Lwt_io.printlf "test_read%!" >>= fun () ->
  let off = 20 in
  let fragment = Fragment.make 666L off 200 in
  let fragments = [ fragment ] in
  let batch = Batch.make fragments in
  IOExecFile.file_open fn [Unix.O_RDONLY] >>= fun handle ->
  IOExecFile.file_read handle batch >>= fun () ->
  Lwt_unix.wait_read event_fd >>= fun () ->
  let buf = Lwt_bytes.create 16 in
  IOExecFile.reap event_fd buf >>= fun (n,ss) ->
  let s0 = List.hd ss in
  Lwt_io.printlf "s0:%s" (IOExecFile.show_status s0) >>= fun () ->
  assert (IOExecFile.get_completion_id s0 = 666L);
  assert (IOExecFile.get_error_code s0 = 0l);
  let data = Fragment.get_bytes fragment in
  check data off 200;
  assert (IOExecFile.get_completion_id s0 = 666L);
  assert (IOExecFile.get_error_code s0 = 0l);
  IOExecFile.file_close handle


let test_delete event_fd fn =
  Lwt_io.printlf "test_delete" >>= fun () ->
  let cid = 999L in
  IOExecFile.file_delete fn cid >>= fun () ->
  let buf = Bytes.create 16 in
  Lwt_unix.wait_read event_fd >>= fun () ->
  Lwt_unix.read event_fd buf 0 16 >>= fun read ->
  Printf.printf "read:%i %S\n%!" read (Bytes.sub buf 0 read);
  Lwt_unix.file_exists fn >>= fun exists ->
  assert (not exists);
  Lwt.return_unit

let main () =

  let t =
    Lwt_io.printlf "start%!" >>= fun () ->

    IOExecFile.init "../src/gioexecfile.conf";
    let event_fd = IOExecFile.get_event_fd () in
    Printf.printf "0%!";
    let align_size = 4096 in
    GMemPool.init align_size;
    let fn = "./xxxx.data" in
    test_write event_fd fn >>= fun () ->
    test_read event_fd fn >>= fun () ->
    test_delete event_fd fn >>= fun () ->
    IOExecFile.destroy ();
    Lwt.return_unit
  in
  Lwt_main.run t


let () = main ()
