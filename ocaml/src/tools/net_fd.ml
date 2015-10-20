type t =
  | Plain of Lwt_unix.file_descr


let wrap fd = Plain fd

let close = function
  | Plain fd -> Lwt_unix.close fd

let to_connection ~in_buffer ~out_buffer = function
  | Plain fd ->
     let ic = Lwt_io.of_fd ~buffer:in_buffer ~mode:Lwt_io.input fd
     and oc = Lwt_io.of_fd ~buffer:out_buffer ~mode:Lwt_io.output fd in
     (ic,oc)

let make_ic ~buffer = function
  | Plain fd -> Lwt_io.of_fd ~buffer ~mode:Lwt_io.input fd

let write_all bytes = function
  | Plain fd -> Lwt_extra2.write_all' fd bytes

let read_all target read remaining = function
  | Plain fd -> Lwt_extra2.read_all fd target read remaining
