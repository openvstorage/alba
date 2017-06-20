(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)


open! Prelude
open OUnit
open Lwt.Infix

let test_fallocate () =
  let t =
    let fn = "./fallocate_test.bin" in
    Lwt.catch
      (fun () -> Lwt_unix.unlink fn)
      (fun _ -> Lwt.return_unit)
    >>= fun () ->
    let len = 4096 in
    let data = Lwt_bytes.create len in
    let () = Lwt_bytes.fill data 0 len 'x' in
    Lwt_extra2.with_fd
      fn
      ~flags:Lwt_unix.([O_WRONLY;O_CREAT;O_EXCL])
      ~perm:0o644
      (fun fd ->
        let mode = 0 in
        Posix.lwt_fallocate fd mode 0 len >>= fun () ->
        Lwt_extra2.write_all_lwt_bytes fd data 0 len >>= fun () ->
        Lwt_unix.fstat fd >>= fun stat ->
        Lwt.return stat.Lwt_unix.st_size
      ) >>= fun actual_size ->
    OUnit.assert_equal len actual_size ~printer:string_of_int ~msg:"wrong file size";
    Lwt_unix.unlink fn >>= fun () ->
    Lwt.return_unit
  in
  Lwt_main.run t

let suite = "posix_test" >::: [
      "test_fallocate" >:: test_fallocate
    ]
