(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open Lwt.Infix
open! Prelude
open Asd_server
open Stat
let post_write_nothing _ _ _ = Lwt.return_unit

let batch_entry_syncfs dir_info fnr data size =
  let t () =
    let blob = Osd.Blob.Bytes data in
    DirectoryInfo.write_blob dir_info fnr blob
                             ~sync_parent_dirs:true
                             ~post_write:post_write_nothing
    >>= fun () ->
    Lwt.return ()
  in
  t

let batch_entry_fsync dir_info fnr data size =
  let flags = [Unix.O_WRONLY; Unix.O_CREAT] in
  let t () =
    let (dir,_,fn) = DirectoryInfo.get_file_dir_name_path dir_info fnr in
    DirectoryInfo.ensure_dir_exists dir_info dir ~sync:true >>= fun () ->
    Lwt_extra2.with_fd fn ~flags ~perm:0o644
    (fun fd ->
      Lwt_unix.write fd data 0 size >>= fun written ->
      assert (written = size);
      Lwt_unix.fsync fd)
  in
  t

let bench_x root entry post_batch iterations n_threads size =

  Lwt_io.printlf
    "bench iterations:%i n_threads:%i size:%i"
    iterations n_threads size
  >>= fun () ->
  let data = Bytes.init size (fun i -> Char.chr (i mod 0xff)) in
  let dir_info = DirectoryInfo.make
                   root
                   ~use_fadvise:false
                   ~use_fallocate:false
  in
  Lwt_unix.openfile root [Unix.O_RDONLY] 0o644 >>= fun dir_fd ->
  let one_batch batch_nr =
    let rec loop ts i =
      if i = 0
      then Lwt.return ts
      else
        begin
          let fnr = Int64.of_int (batch_nr * n_threads + i) in
          let t = entry dir_info fnr data size in
          loop (t :: ts) (i-1)
        end
    in
    loop [] n_threads >>= fun ts ->
    Lwt_list.iter_p (fun p -> p ()) ts >>= fun () ->
    post_batch dir_fd >>= fun rc ->
    assert (rc = 0);
    Lwt.return ()
  in
  let _MB = 1024.0 *. 1024.0 in
  let speed_of d =
    Pervasives.float (n_threads * size)  /. (d *. _MB)
  in
  let stat = Stat.make () in
  let rec repeat stat i =
    if i = iterations
    then Lwt.return stat
    else
      let t0 = Unix.gettimeofday () in
      one_batch i >>= fun () ->
      let t1 = Unix.gettimeofday () in
      let d = t1 -. t0 in
      let stat' = Stat._update stat d in
      let speed = speed_of d in
      Lwt_io.printlf "took:%f => %f MB/s" d speed >>= fun () ->
      repeat stat' (i+1)
  in
  repeat stat 0 >>= fun stat ->
  Lwt_unix.close dir_fd >>= fun () ->
  let worst = stat.Stat.max in
  let avg = stat.Stat.avg in
  Lwt_io.printlf "worst: %f (%f MB/s)" worst (speed_of worst) >>= fun () ->
  Lwt_io.printlf "avg: %f (%f) MB/s" avg (speed_of avg) >>= fun () ->
  Lwt_io.printlf "stat: %s" ([%show :Stat.stat] stat)
