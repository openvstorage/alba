(* nicked from baardskeer (LGPL) and adapted *)

(* pread *)
val pread_into_exactly: Unix.file_descr -> string -> int -> int -> unit
(* pwrite *)
val pwrite_exactly: Unix.file_descr -> string -> int -> int -> unit
(* fsync *)
val fsync: Unix.file_descr -> unit
(* fdatasync *)
val fdatasync: Unix.file_descr -> unit
(* fallocate *)
val fallocate_FALLOC_FL_KEEP_SIZE: unit -> int
val fallocate_FALLOC_FL_PUNCH_HOLE: unit -> int
val fallocate: Unix.file_descr -> int -> int -> int -> unit

(* posix_fadvise *)
type posix_fadv = POSIX_FADV_NORMAL
                  | POSIX_FADV_SEQUENTIAL
                  | POSIX_FADV_RANDOM
                  | POSIX_FADV_NOREUSE
                  | POSIX_FADV_WILLNEED
                  | POSIX_FADV_DONTNEED

val posix_fadvise: Unix.file_descr -> int -> int -> posix_fadv -> unit

(* stat blksize *)
val fstat_blksize: Unix.file_descr -> int

val ioctl_fiemap: Unix.file_descr -> (int64 * int64 * int64 * int32) list

val lwt_posix_fadvise: Lwt_unix.file_descr -> int -> int -> posix_fadv -> unit Lwt.t
