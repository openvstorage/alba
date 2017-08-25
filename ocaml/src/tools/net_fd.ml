(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Lwt.Infix

type transport =
  | TCP
  | RDMA
    [@@deriving show]

type _ssl =
  | Config of (Lwt_unix.file_descr *  Tls.t)
  | Using  of (([ `Server] Typed_ssl.t )* Ssl.socket * Lwt_ssl.socket)

type _ssl_state = { mutable state : _ssl}

let _ssl_get_socket { state } = match state with
  | Config _ -> failwith "no socket yet"
  | Using (_,_,socket) -> socket

let _ssl_get_fd _ssl =
  let socket = _ssl_get_socket _ssl in
  Lwt_ssl.get_fd socket

let _ssl_get_ctx  { state } = match state with
  | Config _ -> failwith "no context yet"
  | Using (ctx,_,_) -> ctx

type t =
  | Plain of Lwt_unix.file_descr
  | SSL of _ssl_state
  | Rsocket of Lwt_rsocket.lwt_rsocket

let identifier = function
  | Plain fd   -> Lwt_extra2.lwt_unix_fd_to_fd fd
  | SSL _ssl   ->
     let fd = _ssl_get_fd _ssl in
     Lwt_extra2.lwt_unix_fd_to_fd fd
  | Rsocket fd -> Lwt_rsocket.identifier fd


let socket domain typ x transport (tls:Tls.t option) =
  match transport with
  | TCP ->
     begin
     match tls with
     | None ->
        let socket = Lwt_unix.socket domain typ x in
        Plain socket
     | Some tls ->
        let fd = Lwt_unix.socket domain typ x in
        let state = Config (fd,tls) in
        SSL { state }
     end
  | RDMA ->
     let socket = Lwt_rsocket.socket domain typ x in
     Rsocket socket

let setsockopt nfd option value =
  match nfd with
  | Plain fd   -> Lwt_unix.setsockopt fd option value
  | SSL _ssl ->
     let fd = _ssl_get_fd _ssl in
     Lwt_unix.setsockopt fd option value
  | Rsocket fd -> Lwt_rsocket.setsockopt fd option value

let bind nfd sa =
  match nfd with
  | Plain fd -> Lwt_unix.bind fd sa
  | SSL _ssl ->
     let fd = _ssl_get_fd _ssl in
     Lwt_unix.bind fd sa
  | Rsocket fd ->
     let r = Lwt_rsocket.bind fd sa in
     Lwt.return r

let listen nfd n =
  match nfd with
  | Plain fd -> Lwt_unix.listen fd n
  | SSL _ssl ->
     let fd = _ssl_get_fd _ssl in
     Lwt_unix.listen fd n;
     let ufd = Lwt_unix.unix_file_descr fd in
     let _ = Ssl.embed_socket ufd in
     ()
  | Rsocket fd -> Lwt_rsocket.listen fd n

let accept = function
  | Plain fd ->
     Lwt_unix.accept fd >>= fun (cl_fd, cl_sa) ->
     Lwt.return (Some (Plain cl_fd, cl_sa))
  | SSL _ssl ->
     let fd = _ssl_get_fd _ssl in
     let ctx = _ssl_get_ctx _ssl in
     Lwt_unix.accept fd >>= fun (cl_fd, cl_sa) ->
     Lwt.catch
       (fun () ->
         Typed_ssl.Lwt.ssl_accept cl_fd ctx >>= fun (x,s) ->
         let r = SSL { state = Using (ctx,x,s)} in
         Lwt.return (Some (r,cl_sa))
       )
       (fun exn ->
        Lwt_log.debug_f ~exn "wrap_socket with context..." >>= fun () ->
        Lwt_unix.close cl_fd >>= fun () ->
        Lwt.return_none
       )
  | Rsocket fd ->
     Lwt_rsocket.accept fd >>= fun (cl_fd,cl_sa) ->
     Lwt.return (Some (Rsocket cl_fd, cl_sa))

let apply_keepalive tcp_keepalive = function
  | Plain fd   -> Tcp_keepalive.apply fd tcp_keepalive
  | SSL _ssl ->
     let fd = _ssl_get_fd _ssl in
     Tcp_keepalive.apply fd tcp_keepalive
  | Rsocket fd -> ()

let string_of_address = function
  | Unix.ADDR_INET(addr, port) ->
     Printf.sprintf
       "ADDR_INET(%s,%i)"
       (Unix.string_of_inet_addr addr) port
  | Unix.ADDR_UNIX s -> Printf.sprintf "ADDR_UNIX %s" s

let connect fd address =
  Lwt_log.info_f "Connecting to %s" (string_of_address address) >>= fun () ->
  match fd with
  | Plain fd   -> Lwt_unix.connect fd address
  | SSL _ssl ->
     begin
       match _ssl.state with
       | Config (fd, tls_config) ->
          Lwt_unix.connect fd address >>= fun () ->
          let ctx = failwith "todo" in
          Typed_ssl.Lwt.ssl_connect fd ctx >>= fun lwt_s ->
          let state' = failwith "todo" in
          _ssl.state <- state';
          Lwt.return_unit
       | Using _ -> failwith "already connected"
  (*
    Lwt_unix.connect fd address >>= fun () ->
    Typed_ssl.Lwt.ssl_connect fd ctx >>= fun lwt_s ->
    let r = Net_fd.wrap_ssl lwt_s in
   *)
     end
  | Rsocket fd ->
     Lwt_log.debug_f "Rsocket.connect"
     >>= fun () ->
     Lwt_rsocket.connect fd address

let close = function
  | Plain socket   -> Lwt_unix.close socket
  | SSL _ssl       -> Lwt_ssl.close (_ssl |> _ssl_get_socket)
  | Rsocket socket -> Lwt_rsocket.close socket

let to_connection ~in_buffer ~out_buffer = function
  | Plain fd ->
     let ic = Lwt_io.of_fd ~buffer:in_buffer ~mode:Lwt_io.input fd
     and oc = Lwt_io.of_fd ~buffer:out_buffer ~mode:Lwt_io.output fd in
     (ic, oc)
  | SSL _ssl ->
     let socket = _ssl_get_socket _ssl in
     let ic = Lwt_ssl.in_channel_of_descr ~buffer:in_buffer socket
     and oc = Lwt_ssl.out_channel_of_descr ~buffer:out_buffer socket in
     (ic, oc)
  | Rsocket socket -> failwith "todo: to_connection (Rsocket rsocket) "

let make_ic ~buffer = function
  | Plain fd       -> Lwt_io.of_fd ~buffer ~mode:Lwt_io.input fd
  | SSL _ssl       -> Lwt_ssl.in_channel_of_descr ~buffer (_ssl_get_socket _ssl)
  | Rsocket socket -> failwith "make_ic rsocket"

let write_all_ssl socket ifd bytes offset length =
  let write_from_source = Lwt_ssl.write socket bytes in
  Lwt_extra2._write_all write_from_source ifd offset length

let write_all nfd bytes offset length = let ifd = identifier nfd in
  match nfd with
  | Plain fd       -> Lwt_extra2.write_all fd bytes offset length
  | SSL _ssl       -> write_all_ssl (_ssl_get_socket _ssl) ifd bytes offset length
  | Rsocket socket ->
     let write_from_source offset todo =
       Lwt_rsocket.send socket bytes offset todo []
     in
     Lwt_extra2._write_all write_from_source ifd offset length

let write_all' nfd bytes = write_all nfd bytes 0 (Bytes.length bytes)

let write_all_lwt_bytes nfd bs offset length = let ifd = identifier nfd in
  match nfd with
  | Plain fd -> Lwt_extra2.write_all_lwt_bytes fd bs offset length
  | SSL _ssl ->
     let socket = _ssl_get_socket _ssl in
     Lwt_extra2._write_all
       (Lwt_ssl.write_bytes socket bs)
       ifd offset length
  | Rsocket socket ->
     let write_from_source offset todo =
       Lwt_rsocket.Bytes.send socket bs offset todo []
     in
     Lwt_extra2._write_all write_from_source ifd offset length

let read_all nfd target offset length = let ifd = identifier nfd in
  match nfd with
  | Plain fd -> Lwt_extra2.read_all fd target offset length
  | SSL _ssl ->
     let socket = _ssl_get_socket _ssl in
     let read_to_target = Lwt_ssl.read socket target in
     Lwt_extra2._read_all read_to_target ifd offset length
  | Rsocket socket ->
     Lwt_log.debug_f "Rsocket.read_all _ _ %i %i" offset length >>= fun () ->
     let read_to_target offset todo =
       Lwt_log.debug_f "Rsocket.recv %i %i" offset todo >>= fun () ->
       Lwt_rsocket.recv socket target offset todo [] >>= fun read ->
       Lwt_log.debug_f "Rsocket.recv %i" read >>= fun () ->
       Lwt.return read
     in
     Lwt_extra2._read_all read_to_target ifd offset length

let read_all_exact nfd target offset length =
  read_all nfd target offset length
  >>= Lwt_extra2.expect_exact_length length

let read_all_lwt_bytes_exact nfd target offset length =
  let ifd = identifier nfd in
  match nfd with
  | Plain fd -> Lwt_extra2.read_all_lwt_bytes_exact fd target offset length
  | SSL _ssl ->
     let socket = _ssl_get_socket _ssl in
     Lwt_extra2._read_all
       (Lwt_ssl.read_bytes socket target)
       ifd offset length
     >>= Lwt_extra2.expect_exact_length length
  | Rsocket socket ->
     Lwt_log.debug_f "Rsocket.read_all _ _ %i %i" offset length >>= fun () ->
     let read_to_target offset todo =
       Lwt_log.debug_f "Rsocket.Bytes.recv _ _ %i %i" offset length >>= fun () ->
       Lwt_rsocket.Bytes.recv socket target offset todo []
       >>= fun read ->
       Lwt_log.debug_f "Rsocket.Bytes.recv %i bytes" read >>= fun () ->
       Lwt.return read
     in
     Lwt_extra2._read_all read_to_target ifd offset length
     >>= Lwt_extra2.expect_exact_length length

let read_lwt_bytes_at_least nfd target ~offset ~max_length ~min_length =
  match nfd with
  | Plain fd -> Lwt_extra2.read_lwt_bytes_at_least fd target ~offset ~max_length ~min_length
  | SSL _ssl ->
     let socket = _ssl_get_socket _ssl in
     Lwt_extra2._read_buffer_at_least
       (Lwt_ssl.read_bytes socket target)
       ~offset ~max_length ~min_length
  | Rsocket socket ->
     Lwt_extra2._read_buffer_at_least
       (fun offset length -> Lwt_rsocket.Bytes.recv socket target offset length [])
       ~offset ~max_length ~min_length


let with_maybe_cancel cancel f =
  match cancel with
  | None -> f ()
  | Some cancel ->
     Lwt.pick
       [ (Lwt_condition.wait cancel >>= fun () ->
          Lwt.fail Lwt.Canceled);
         f (); ]


let with_message_buffer_from
      nfd
      buffer
      ~max_buffer_size
      cancel
      f
  =
  let buf_length = Lwt_bytes.length !buffer in
  with_maybe_cancel
    cancel
    (fun () ->
      read_lwt_bytes_at_least
        nfd !buffer
        ~offset:0
        ~max_length:buf_length
        ~min_length:4)
  >>= fun read ->

  let message_length = get32_prim' !buffer 0 |> Int32.to_int in
  let message_plus_header_length = message_length + 4 in

  let with_message_buffer f =
    if message_plus_header_length <= read
    then
      (* read the message entirely - and maybe then some - perfect *)
      f
        ~buffer:!buffer
        ~offset:4
        ~message_length
        ~extra_bytes:(read - message_plus_header_length)
    else
      let message_bytes_read = read - 4 in

      let with_sufficiently_large_buffer f =
        if message_plus_header_length <= buf_length
        then
          f (!buffer, 4)
        else
          begin
            let new_buf =
              Lwt_bytes.create
                ~msg:"Net_fd.new_buf"
                message_plus_header_length
            in
            Lwt_bytes.blit !buffer 4 new_buf 0 message_bytes_read;

            if message_plus_header_length <= max_buffer_size
            then
              begin
                Lwt_bytes.unsafe_destroy !buffer;
                buffer := new_buf;
                f (new_buf, 0)
              end
            else
              begin
                Lwt.finalize
                  (fun () -> f (new_buf, 0))
                  (fun () ->
                    Lwt_bytes.unsafe_destroy new_buf;
                    Lwt.return_unit)
              end
          end
      in

      (* only got part of the message so far
       * gather the message into a sufficiently large buffer
       * (maybe blit, get the rest from nfd)
       *)
      with_sufficiently_large_buffer
        (fun (buffer, offset) ->
          read_all_lwt_bytes_exact
            nfd buffer
            (offset + message_bytes_read)
            (message_length - message_bytes_read) >>= fun () ->
          f
            ~buffer
            ~offset
            ~message_length
            ~extra_bytes:0)
  in

  with_message_buffer f

let cork = function
  | Plain fd ->
     Lwt_unix.setsockopt fd Lwt_unix.TCP_NODELAY false
  | SSL _ssl ->
     let fd = _ssl_get_fd _ssl in
     Lwt_unix.setsockopt
       fd
       Lwt_unix.TCP_NODELAY false
  | Rsocket fd  ->
     Lwt_rsocket.setsockopt fd Lwt_unix.TCP_NODELAY false

let uncork = function
  | Plain fd   ->
     Lwt_unix.setsockopt fd Lwt_unix.TCP_NODELAY true
  | SSL _ssl ->
     let fd = _ssl_get_fd _ssl in
     Lwt_unix.setsockopt
       fd
       Lwt_unix.TCP_NODELAY true
  | Rsocket socket ->
     Lwt_rsocket.setsockopt socket Lwt_unix.TCP_NODELAY true
