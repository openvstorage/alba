(* nicked from baardskeerder (LGPL) and adapted *)

(* posix_fadvise *)
type posix_fadv = POSIX_FADV_NORMAL
                  | POSIX_FADV_SEQUENTIAL
                  | POSIX_FADV_RANDOM
                  | POSIX_FADV_NOREUSE
                  | POSIX_FADV_WILLNEED
                  | POSIX_FADV_DONTNEED

val posix_fadvise: Unix.file_descr -> int -> int -> posix_fadv -> unit

val lwt_posix_fadvise: Lwt_unix.file_descr -> int -> int -> posix_fadv -> unit Lwt.t
