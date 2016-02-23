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

class type reader = object
  method reset : unit Lwt.t
  method length : int Lwt.t
  (* reads exactly the amount of requested bytes or gives an error *)
  method read : int -> Lwt_bytes.t -> unit Lwt.t
end

class file_reader fd size = (object
  method reset =
    Lwt.map ignore (Lwt_unix.lseek fd 0 Lwt_unix.SEEK_SET)

  method length =
    Lwt_unix.fstat fd >>= fun stat ->
    Lwt.return stat.Lwt_unix.st_size

  method read cnt target =
    Lwt_extra2.read_all_lwt_bytes fd target 0 cnt >>= fun read ->
    assert (read = cnt);
    Lwt.return_unit
end : reader)

let with_file_reader input_file f =
  Lwt_extra2.with_fd
    input_file
    ~flags:Lwt_unix.([O_RDONLY;])
    ~perm:0o600
    (fun fd ->
      Lwt_unix.fstat fd >>= fun stat ->
      let size = stat.Lwt_unix.st_size in
      Posix.lwt_posix_fadvise fd 0 size Posix.POSIX_FADV_SEQUENTIAL
      >>= fun () ->
     let object_reader = new file_reader fd size in
     f ~object_reader
     >>= fun r ->
     Posix.lwt_posix_fadvise fd 0 size Posix.POSIX_FADV_DONTNEED >>= fun () ->
     Lwt.return r
    )

class string_reader object_data = (object
  val obj_len = String.length object_data
  val mutable pos = 0

  method reset =
    pos <- 0;
    Lwt.return_unit

  method length =
    Lwt.return obj_len

  method read cnt target =
    Lwt_bytes.blit_from_bytes object_data pos target 0 cnt;
    pos <- pos + cnt;
    Lwt.return_unit
end : reader)

class bytes_reader object_data = (object
  val obj_len = Lwt_bytes.length object_data
  val mutable pos = 0

  method reset =
    pos <- 0;
    Lwt.return_unit

  method length =
    Lwt.return obj_len

  method read cnt target =
    Lwt_bytes.blit object_data pos target 0 cnt;
    pos <- pos + cnt;
    Lwt.return_unit
end : reader)
