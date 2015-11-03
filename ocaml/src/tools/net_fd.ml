type t =
  | Plain of Lwt_unix.file_descr
  | SSL of (Ssl.socket * Lwt_ssl.socket)

let wrap_plain fd = Plain fd

let wrap_ssl socket = SSL socket


let close = function
  | Plain fd -> Lwt_unix.close fd
  | SSL (x, socket) -> Lwt_ssl.close socket

let to_connection ~in_buffer ~out_buffer = function
  | Plain fd ->
     let ic = Lwt_io.of_fd ~buffer:in_buffer ~mode:Lwt_io.input fd
     and oc = Lwt_io.of_fd ~buffer:out_buffer ~mode:Lwt_io.output fd in
     (ic, oc)
  | SSL (_, socket) ->
     let ic = Lwt_ssl.in_channel_of_descr ~buffer:in_buffer socket
     and oc = Lwt_ssl.out_channel_of_descr ~buffer:out_buffer socket in
     (ic, oc)

let make_ic ~buffer = function
  | Plain fd       -> Lwt_io.of_fd ~buffer ~mode:Lwt_io.input fd
  | SSL (_,socket) -> Lwt_ssl.in_channel_of_descr ~buffer socket


let write_all_ssl socket bytes offset length =
  let write_from_source = Lwt_ssl.write socket bytes in
  Lwt_extra2._write_all write_from_source offset length

let write_all bytes = function
  | Plain fd       -> Lwt_extra2.write_all' fd bytes
  | SSL (_,socket) ->
     let offset = 0
     and length = Bytes.length bytes
     in write_all_ssl socket bytes offset length

let read_all target read remaining = function
  | Plain fd -> Lwt_extra2.read_all fd target read remaining
  | SSL (_,socket) ->
     let read_to_target = Lwt_ssl.read socket target in
     Lwt_extra2._read_all read_to_target read remaining
