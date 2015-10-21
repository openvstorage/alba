type t =
  | Plain of Lwt_unix.file_descr
  | SSL of (Ssl.socket * Lwt_ssl.socket)

let wrap fd = Plain fd

let wrap_ssl socket = SSL socket

let close = function
  | Plain fd -> Lwt_unix.close fd
  | SSL (x, socket) -> Lwt_ssl.close socket

let to_connection ~in_buffer ~out_buffer = function
  | Plain fd ->
     let ic = Lwt_io.of_fd ~buffer:in_buffer ~mode:Lwt_io.input fd
     and oc = Lwt_io.of_fd ~buffer:out_buffer ~mode:Lwt_io.output fd in
     (ic, oc)
  | SSL (_,socket) ->
     let ic = Lwt_ssl.in_channel_of_descr ~buffer:in_buffer socket
     and oc = Lwt_ssl.out_channel_of_descr ~buffer:out_buffer socket in
     (ic, oc)

let make_ic ~buffer = function
  | Plain fd -> Lwt_io.of_fd ~buffer ~mode:Lwt_io.input fd
  | SSL (_,socket) -> failwith "SSL.make_ic"

let write_all bytes = function
  | Plain fd -> Lwt_extra2.write_all' fd bytes
  | SSL (_,socket) -> Lwt.fail_with "SSL.write_all"

let read_all target read remaining = function
  | Plain fd -> Lwt_extra2.read_all fd target read remaining
  | SSL (_,socket) -> Lwt.fail_with "SSL.read_all"
