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

open Lwt.Infix

let fill data =
  let len = Lwt_bytes.length data in
  let rec loop n =
    if n = len
    then ()
    else
      let c = Char.chr (n mod 100) in
      let () = Lwt_bytes.set data n c in
      loop (n+1)
  in
  loop 0

let check data boff corr len =
  let rec loop n =
    if n = len
    then ()
    else
      let pos = boff + n in
      let c = Lwt_bytes.get data pos in
      let c' = Char.chr((corr + n) mod 100) in
      let printer c = Printf.sprintf "%C:0x%02x" c (Char.code c) in
      let msg = Printf.sprintf "buffer.(%i) = 0x%02x" pos (Char.code c) in
      let () = OUnit.assert_equal ~msg ~printer c' c in
      loop (n+1)
  in
  loop 0


let _test_write dir_info lwt_bytes =
  let blob = Asd_protocol.Blob.Lwt_bytes lwt_bytes in

  let fnr = 0L in
  let nothing fdo _ _ = Lwt.return_unit in

  dir_info # write_blob fnr blob ~post_write:nothing ~sync_parent_dirs:true >>= fun () ->
  let files_path,dir,file_name = dir_info # _get_file_dir_name_path fnr in
  Lwt_unix.file_exists file_name >>= fun exists ->
  OUnit.assert_bool "file exists" exists;
  Lwt_unix.stat file_name >>= fun stats ->
  let file_size = stats.Unix.st_size in
  OUnit.assert_equal ~msg:"file size" file_size (5 * 4096);
  Lwt.return_unit


let _test_read dir_info lwt_bytes =
  let size = Lwt_bytes.length lwt_bytes in
  let fnr = 0L in
  dir_info # get_blob fnr size >>= fun (bytes:string) ->
  let lwt_bytes = Lwt_bytes.of_string bytes in
  OUnit.assert_equal ~msg:"blob_size" size (String.length bytes);
  check lwt_bytes 0 0 size;
  Lwt.return_unit

let _test_partial_read dir_info lwt_bytes =
  let fnr = 0L in
  let do_one slices =
    let callback (off', len') buffer boff =
      Lwt_io.printlf "callback (%i,%i) (_:%i bytes) boff:%i"
                     off' len' (Lwt_bytes.length buffer) boff
      >>= fun () ->
      check buffer boff off' len';
      Lwt.return_unit
    in
    let len = List.length slices in
    dir_info # get_blob_data fnr len slices callback
  in
  Lwt_list.iter_s do_one [
                    [(4096,     1)];
                    [(4200, 5_000)];
                    [( 100,    10)];
                    [(4090,    16)];
                    [(0,100); (200,300);(400,500)]
                  ]

let _test_delete dir_info lwt_bytes =
  let fnr = 0L in
  dir_info # delete_blobs [fnr] ~ignore_unlink_error:false >>= fun () ->
  Lwt.return_unit

let _test_generic dir_info =
  let size = 20_000 in
  let lwt_bytes = Lwt_bytes.create size in
  let () = fill lwt_bytes in
  _test_write dir_info lwt_bytes >>= fun () ->
  _test_read dir_info lwt_bytes >>= fun () ->
  _test_partial_read dir_info lwt_bytes >>= fun () ->
  _test_delete dir_info lwt_bytes >>= fun () ->
  Lwt_io.printlf "end of test%!"

let test_generic () =
  let setup dir =
    let cmd0 = Printf.sprintf "rm -rf %s" dir in
    let cmd1 = Printf.sprintf "mkdir -p %s" dir in
    let _rc = Sys.command cmd0 in
    let _rc = Sys.command cmd1 in
    ()
  in
  let t =
    let files_path = "/tmp/blob_access_test/" in
    setup files_path;
    let use_fadvise = true
    and use_fallocate = true
    in
    let engine = Asd_config.Config.GioExecFile "./cfg/gioexecfile.conf" in
    let statistics = Asd_statistics.AsdStatistics.make () in
    let dir_info =
      Blob_access_factory.make_directory_info
        ~engine
        ~statistics
        files_path
        ~use_fadvise
        ~use_fallocate
    in
    _test_generic dir_info >>= fun () ->
    Blob_access_factory.endgame ();
    Lwt.return_unit
  in
  Lwt_main.run t


let suite =
  let open OUnit in
  "gblob_access" >::: ["generic" >:: test_generic]
