(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Slice
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

let _with_file_reader ~use_fadvise file_name f =
  Lwt_extra2.with_fd
    file_name
    ~flags:Lwt_unix.([O_RDONLY;])
    ~perm:0o600
    (fun fd ->
      Lwt_unix.fstat fd >>= fun stat ->
      let size = stat.Lwt_unix.st_size in
      let ufd = Lwt_unix.unix_file_descr fd in
      let () = if use_fadvise then Posix.posix_fadvise ufd 0 size Posix.POSIX_FADV_SEQUENTIAL in
      let object_reader = new file_reader fd size in
      f ~object_reader
      >>= fun r ->
      let () = if use_fadvise then Posix.posix_fadvise ufd 0 size Posix.POSIX_FADV_DONTNEED in
      Lwt.return r
    )

let with_file_reader ~use_fadvise file_name f =
  Lwt.catch
    (fun () -> _with_file_reader ~use_fadvise file_name f)
    (function
     | Unix.Unix_error(Unix.ENOENT,_,_) -> Alba_client_errors.Error.(lwt_failwith FileNotFound)
     | exn -> Lwt.fail exn
    )

class slice_reader object_data =
object(self :# reader)
  val obj_len = Slice.length object_data
  val mutable pos = 0

  method reset =
    pos <- 0;
    Lwt.return_unit

  method length =
    Lwt.return obj_len

  method read cnt target =
    Lwt_bytes.blit_from_bytes
      object_data.Slice.buf (pos + object_data.Slice.offset)
      target 0
      cnt;
    pos <- pos + cnt;
    Lwt.return_unit
end

class string_reader object_data =
object
  inherit slice_reader (Slice.wrap_string object_data)
end

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

class bigstring_slice_reader object_data =
  (object
      val mutable pos = 0

      method reset =
        pos <- 0;
        Lwt.return_unit

      method length =
        Lwt.return (Bigstring_slice.length object_data)

      method read cnt target =
        Lwt_bytes.blit
          object_data.Bigstring_slice.bs (object_data.Bigstring_slice.offset + pos)
          target 0
          cnt;
        pos <- pos + cnt;
        Lwt.return_unit
    end : reader)
