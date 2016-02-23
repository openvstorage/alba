(* nicked from baardskeerder (LGPL) and adapted *)


(* posix_fadvise *)
type posix_fadv = POSIX_FADV_NORMAL
                  | POSIX_FADV_SEQUENTIAL
                  | POSIX_FADV_RANDOM
                  | POSIX_FADV_NOREUSE
                  | POSIX_FADV_WILLNEED
                  | POSIX_FADV_DONTNEED

external posix_fadvise: Unix.file_descr -> int -> int -> posix_fadv -> unit
  = "_bs_posix_fadvise"

let lwt_posix_fadvise fd pos len fadv =
  let ufd = Lwt_unix.unix_file_descr fd in
  Lwt_preemptive.detach
    (fun () -> posix_fadvise ufd 0 len fadv) ()

