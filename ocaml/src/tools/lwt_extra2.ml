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

open Lwt.Infix

let ignore_errors ?(logging=false) f =
  Lwt.catch
    f
    (function
      | Lwt.Canceled -> Lwt.fail Lwt.Canceled
      | exn ->
         if logging
         then Lwt_log.debug ~exn "ignoring"
         else Lwt.return ())

let with_timeout ~msg (timeout:float) f =
  Lwt.catch
    (fun () -> Lwt_unix.with_timeout timeout f)
    (fun ex ->
     begin
       match ex with
       | Lwt_unix.Timeout -> Lwt_log.debug_f "Timeout(%.2f):%s" timeout msg
       | _ -> Lwt.return_unit
     end >>= fun () -> Lwt.fail ex
    )

module CountDownLatch = struct
  type t = { mutable needed : int;
             mutable waiters : unit Lwt.u list; }

  let create ~count = { needed = count; waiters = [] }

  let count_down t =
    if t.needed <> 0
    then
      t.needed <- t.needed - 1;
    if t.needed = 0
    then begin
      List.iter (fun lwtu -> Lwt.wakeup_later lwtu ()) t.waiters;
      t.waiters <- []
    end

  let finished t = t.needed = 0

  let await t =
    if finished t
    then Lwt.return ()
    else
      let lwtt, lwtu = Lwt.wait () in
      t.waiters <- lwtu :: t.waiters;
      lwtt
end

module FoldingCountDownLatch = struct
  type ('a, 'b) t = {
    latch : CountDownLatch.t;
    mutable acc : 'a;
    f : 'a -> 'b -> 'a; }

  let create ~count ~acc ~f =
    { latch = CountDownLatch.create ~count;
      acc; f; }

  let notify t b =
    if t.latch.CountDownLatch.needed <> 0
    then begin
      t.acc <- t.f t.acc b;
      CountDownLatch.count_down t.latch
    end

  let await t =
    let open Lwt in
    CountDownLatch.await t.latch >>= fun () ->
    Lwt.return t.acc
end

let sleep_approx ?(jitter=0.1) duration =
  let delta = Random.float (2. *. jitter) -. jitter in
  Lwt_unix.sleep ((1. +. delta) *. duration)

let rec run_forever msg f delay =
  Lwt.catch
    f
    (fun exn -> Lwt_log.debug ~exn msg) >>= fun () ->
  sleep_approx delay >>= fun () ->
  run_forever msg f delay

let lwt_unix_fd_to_fd
      (fd : Lwt_unix.file_descr) : int =
  Obj.magic (Obj.field (Obj.repr fd) 0)

let lwt_unix_fd_to_unix_fd
      (fd : Lwt_unix.file_descr) : Unix.file_descr =
  Obj.magic (Obj.field (Obj.repr fd) 0)

let unix_fd_to_fd (fd : Unix.file_descr) : int =
  Obj.magic fd

let with_fd filename ~flags ~perm f =
  Lwt_unix.openfile filename flags perm >>= fun fd ->
  Lwt.finalize
    (fun () -> f fd)
    (fun () -> Lwt_unix.close fd)

let fsync_dir dir =
  Lwt_unix.openfile dir [Unix.O_RDONLY] 0640 >>= fun dir_descr ->
  Lwt.finalize
    (fun () -> Lwt_unix.fsync dir_descr)
    (fun () -> Lwt_unix.close dir_descr)

let fsync_dir_of_file filename =
  fsync_dir (Filename.dirname filename)

let create_dir ?(sync = true) dir =
  Lwt_unix.mkdir dir 0o775 >>= fun () ->
  if sync
  then fsync_dir_of_file dir
  else Lwt.return ()

let unlink ?(may_not_exist=false) ~fsync_parent_dir file =
  Lwt.catch
    (fun () ->
     Lwt_unix.unlink file >>= fun () ->
     if fsync_parent_dir
     then fsync_dir_of_file file
     else Lwt.return ())
    (function
      | Unix.Unix_error(Unix.ENOENT, _, _) as exn ->
        if may_not_exist
        then Lwt.return_unit
        else Lwt.fail exn
      | exn ->
        Lwt.fail exn)

let rename ~fsync_parent_dir from dest =
  Lwt_unix.rename from dest >>= fun () ->
  if fsync_parent_dir
  then fsync_dir_of_file dest
  else Lwt.return ()

let _read_all read_to_target offset length =
  let rec inner offset count = function
    | 0 ->
       begin
         if count < 2
         then Lwt.return_unit
         else Lwt_log.info_f "reading from fd: %iB in %i steps" length count
       end >>= fun () ->
       Lwt.return length
    | todo ->
       read_to_target offset todo >>= function
       | 0 -> Lwt.return (length - todo)
       | got -> inner (offset + got) (count + 1) (todo - got)
  in
  inner offset 0 length

let read_all_lwt_bytes fd target offset length =
  _read_all (Lwt_bytes.read fd target) offset length

let expect_exact_length len length =
  if len = length
  then Lwt.return_unit
  else Lwt.fail End_of_file

let read_all_lwt_bytes_exact fd target offset length =
  read_all_lwt_bytes fd target offset length >>= expect_exact_length length

let read_all fd target offset length =
  _read_all (Lwt_unix.read fd target) offset length

let read_all_exact fd target offset length =
  read_all fd target offset length >>= expect_exact_length length

let _write_all write_from_source offset length =
  let rec inner offset todo =
    if todo = 0
    then Lwt.return ()
    else
      write_from_source offset todo >>= fun written ->
      (* only log if we can't push it all out in 1 go *)
      if written = todo
      then Lwt.return ()
      else
        Lwt_log.info_f "writing to fd: written %i, requested %i"
                       written todo
        >>= fun () ->
        inner (offset + written) (todo - written)
  in
  inner offset length

let write_all_lwt_bytes fd source offset length =
  _write_all (Lwt_bytes.write fd source) offset length

let write_all fd source offset length =
  _write_all (Lwt_unix.write fd source) offset length

let write_all' fd source =
  write_all fd source 0 (Bytes.length source)

let llio_output_and_flush oc s =
  Llio.output_string oc s >>= fun () ->
  Lwt_io.flush oc


let make_fuse_thread () =
  let mvar = Lwt_mvar.create_empty () in
  let signals = [ (Sys.sigterm, "SIGTERM");
                  (Sys.sigint, "SIGINT"); ] in
  let signal_handler x =
    Lwt.ignore_result (Lwt_mvar.put mvar x)
  in
  List.iter
    (fun (signal, _) -> Lwt_unix.on_signal signal signal_handler |> ignore)
    signals;
  Lwt_mvar.take mvar >>= fun x ->
  let sig_name = List.assoc x signals in
  Lwt_log.warning_f "Got signal %s (%i), terminating..." sig_name x >>= fun () ->
  (* this statement flushes the log output stream (aka stdout) *)
  Lwt_io.printf "%!"

let get_files_of_directory dir =
  Lwt_stream.to_list (Lwt_unix.files_of_directory dir) >>= fun l ->
  let res =
    List.filter
      (fun fn -> not (List.mem fn [ Filename.parent_dir_name;
                                    Filename.current_dir_name; ]))
      l
  in
  Lwt.return res

let read_file file =
  Lwt_io.file_length file >>= fun len ->
  let len' = Int64.to_int len in
  let buf = Bytes.create len' in
  with_fd
    file
    ~flags:Lwt_unix.([O_RDONLY;])
    ~perm:0600
    (fun fd ->
       read_all fd buf 0 len' >>= fun read ->
       assert (read = len');
       Lwt.return ()) >>= fun () ->
  Lwt.return buf
