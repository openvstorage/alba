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

(*
SYNOPSIS
       #include <sys/statvfs.h>

       int statvfs(const char *path, struct statvfs *buf);
       int fstatvfs(int fd, struct statvfs *buf);

DESCRIPTION
       The function statvfs() returns information about a mounted file system.
       path is the pathname of any file within the mounted file  system.   buf
       is a pointer to a statvfs structure defined approximately as follows:

           struct statvfs {
               unsigned long  f_bsize;    /* file system block size */
               unsigned long  f_frsize;   /* fragment size */
               fsblkcnt_t     f_blocks;   /* size of fs in f_frsize units */
               fsblkcnt_t     f_bfree;    /* # free blocks */
               fsblkcnt_t     f_bavail;   /* # free blocks for unprivileged users */
               fsfilcnt_t     f_files;    /* # inodes */
               fsfilcnt_t     f_ffree;    /* # free inodes */
               fsfilcnt_t     f_favail;   /* # free inodes for unprivileged users */
               unsigned long  f_fsid;     /* file system ID */
               unsigned long  f_flag;     /* mount flags */
               unsigned long  f_namemax;  /* maximum filename length */
           };

// so what I need is:
 'free space' as f_bsize * f_bfree.

     *)
(* nicked from:
    https://github.com/mcclurmc/ocaml-statvfs/blob/master/lib/statvfs.ml

Copyright (c) 2013, Mike McClurg
Permission to use, copy, modify, and/or distribute this software for
any purpose with or without fee is hereby granted, provided that the
above copyright notice and this permission notice appear in all
copies.
THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL
DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR
PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
*)

(* yes, we copied & modified & added stuff *)

    open Ctypes
    open Foreign
    type mount_flag = ST_RDONLY | ST_NOSUID
    type statvfs = {
        f_bsize : int64;
        f_frsize : int64;
        f_blocks : int64;
        f_bfree : int64;
        f_bavail : int64;
        f_files : int64;
        f_ffree : int64;
        f_favail : int64;
        f_flag : mount_flag list;
        f_namemax : int64;
      }
    type statvfs_t
    let statvfs_t : statvfs structure typ = Ctypes_static.structure "statvfs"
    let fsblkcnt_t = uint64_t
    let fsfilcnt_t = uint64_t
    let f_bsize = field statvfs_t "f_bsize" uint64_t
    let f_frsize = field statvfs_t "f_frsize" uint64_t
    let f_blocks = field statvfs_t "f_blocks" fsblkcnt_t
    let f_bfree = field statvfs_t "f_bfree" fsblkcnt_t
    let f_bavail = field statvfs_t "f_bavail" fsblkcnt_t
    let f_files = field statvfs_t "f_files" fsfilcnt_t
    let f_ffree = field statvfs_t "f_ffree" fsfilcnt_t
    let f_favail = field statvfs_t "f_favail" fsblkcnt_t
    let f_fsid = field statvfs_t "f_fsid" uint64_t
    let f_flag = field statvfs_t "f_flag" uint64_t
    let f_namemax = field statvfs_t "f_namemax" uint64_t
    let f__spare = field statvfs_t "f_spare" (array 6 uint32_t)
    let () = seal statvfs_t
    (* TODO *)
    let flags_of_uint64 _ = []
    (* XXX why isn't this a view in Ctypes? *)
    let int64_of_t ty n =
      match sizeof ty with
      | 4 ->
         (Obj.magic n : Unsigned.UInt32.t) |> Unsigned.UInt32.to_int32 |> Int64.of_int32
      | 8 ->
         (Obj.magic n : Unsigned.UInt64.t) |> Unsigned.UInt64.to_int64
      | _ -> failwith "invalid conversion size"
    let from_statvfs_t statvfs = {
        f_bsize = getf statvfs f_bsize |> Unsigned.UInt64.to_int64;
        f_frsize = getf statvfs f_frsize |> Unsigned.UInt64.to_int64;
        f_blocks = getf statvfs f_blocks |> int64_of_t fsblkcnt_t;
        f_bfree = getf statvfs f_bfree |> int64_of_t fsblkcnt_t;
        f_bavail = getf statvfs f_bavail |> int64_of_t fsblkcnt_t;
        f_files = getf statvfs f_files |> int64_of_t fsfilcnt_t;
        f_ffree = getf statvfs f_ffree |> int64_of_t fsfilcnt_t;
        f_favail = getf statvfs f_favail |> int64_of_t fsfilcnt_t;
        f_flag = getf statvfs f_flag |> flags_of_uint64;
        f_namemax = getf statvfs f_namemax |> Unsigned.UInt64.to_int64;
      }
    let statvfs =
      foreign ~check_errno:true "statvfs" (string @-> ptr statvfs_t @-> returning int)
    let statvfs f =
      let s = make statvfs_t in
      statvfs f (addr s) |> ignore;
      from_statvfs_t s
    let fstatvfs =
      foreign ~check_errno:true "fstatvfs" (int @-> ptr statvfs_t @-> returning int)
    let fstatvfs fd =
      let s = make statvfs_t in
      fstatvfs (Obj.magic fd) (addr s) |> ignore;
      from_statvfs_t s
    let ( *: ) = Int64.mul
    and ( /: ) = Int64.div
    let (+:) x y = Int64.add x y
    let (-:) x y = Int64.sub x y

    let free_blocks ?bs path =
      let s = statvfs path in
      match bs with
      | None -> s.f_bavail
      | Some bs ->
         (s.f_bavail *: s.f_frsize) /: (Int64.of_int bs)

    let free_bytes path = free_blocks ~bs:1 path

    let size_blocks ?bs path =
      let s = statvfs path in
      match bs with
      | None -> s.f_blocks
      | Some bs ->
         (s.f_blocks *: s.f_frsize) /: (Int64.of_int bs)
    let size_bytes path = size_blocks ~bs:1 path
    let used_blocks ?(bs=1024) path =
      (size_blocks ~bs path) -: (free_blocks ~bs path)

    let used_bytes path = used_blocks ~bs:1 path
    let block_size path =
      let s = statvfs path in
      Int64.to_int s.f_frsize


    let free_space path =
      let s = statvfs path in
      (s.f_bsize) *: (s.f_bfree)

    let percentage_used s =
      let used_blocks = s.f_blocks -: s.f_bfree in
      100.0 *. (Int64.to_float used_blocks) /.
        (Int64.to_float (used_blocks +: s.f_bavail))

let disk_usage home =
  let s = statvfs home in
  let used = s.f_blocks -: s.f_bfree in
  let total = used +: s.f_bavail in
  let size = s.f_frsize in
  let used_b = used *: size in
  let total_b = total *: size in
  (used_b, total_b)

let lwt_disk_usage home =
  Lwt_preemptive.detach disk_usage home

let lwt_unix_fd_to_fd
      (fd : Lwt_unix.file_descr) : int =
  Obj.magic (Obj.field (Obj.repr fd) 0)

let lwt_unix_fd_to_unix_fd
      (fd : Lwt_unix.file_descr) : Unix.file_descr =
  Obj.magic (Obj.field (Obj.repr fd) 0)

let unix_fd_to_fd (fd : Unix.file_descr) : int =
  Obj.magic fd

let sendfile ~release_runtime_lock =
  let inner =
    (* ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count); *)
    foreign
      "sendfile"
      ~check_errno:true
      ~release_runtime_lock
      (int @-> int @-> ptr void @-> size_t @-> returning size_t)
  in
  fun ~fd_in ~fd_out len ->
  let res = inner fd_out fd_in null (Unsigned.Size_t.of_int len) in
  Unsigned.Size_t.to_int res

let sendfile_all ~fd_in ~fd_out =
  let open Lwt.Infix in
  let fd_in' = lwt_unix_fd_to_fd fd_in in
  let fd_out' = lwt_unix_fd_to_fd fd_out in
  let rec inner = function
    | 0 -> Lwt.return ()
    | n ->
       Lwt_unix.wait_read fd_in >>= fun () ->
       Lwt_unix.wait_write fd_out >>= fun () ->
       let sent =
         sendfile
           ~release_runtime_lock:false
           ~fd_in:fd_in'
           ~fd_out:fd_out'
           n
       in
       inner (n - sent)
  in
  inner
