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

type transport =
  | TCP
  | RDMA
    [@@deriving show]
    
type t =
  | Plain of Lwt_unix.file_descr
  | SSL of (([ `Server] Typed_ssl.t )* Ssl.socket * Lwt_ssl.socket)
  | Rsocket of Lwt_rsocket.lwt_rsocket

let identifier = function
  | Plain fd     -> Lwt_extra2.lwt_unix_fd_to_fd fd
  | SSL (_,_,socket) -> Lwt_extra2.lwt_unix_fd_to_fd (Lwt_ssl.get_fd socket)
  | Rsocket fd -> let (r :int ) = Obj.magic fd in
                  r
                 
let socket domain typ x transport =
  match transport with
  | TCP ->
     let socket = Lwt_unix.socket domain typ x in
     Plain socket
  | RDMA ->
     let socket = Lwt_rsocket.socket domain typ x in
     Rsocket socket

let setsockopt nfd option value =
  match nfd with
  | Plain fd   -> Lwt_unix.setsockopt fd option value
  | SSL(_,_,socket) ->
     let fd = Lwt_ssl.get_fd socket in
     Lwt_unix.setsockopt fd option value
  | Rsocket fd -> Lwt_rsocket.setsockopt fd option value

let bind nfd sa =
  match nfd with
  | Plain fd -> Lwt_unix.bind fd sa
  | SSL(_,_,socket) ->
     let fd = Lwt_ssl.get_fd socket in
     Lwt_unix.bind fd sa
  | Rsocket fd -> Lwt_rsocket.bind fd sa

let listen nfd n =
  match nfd with
  | Plain fd -> Lwt_unix.listen fd n
  | SSL (_ctx,_,socket) ->
     let fd = Lwt_ssl.get_fd socket in
     Lwt_unix.listen fd n;
     let ufd = Lwt_ssl.get_unix_fd socket in
     let _ = Ssl.embed_socket ufd in
     ()
  | Rsocket fd -> Lwt_rsocket.listen fd n

let accept = function
  | Plain fd ->
     Lwt_unix.accept fd >>= fun (cl_fd, cl_sa) ->
     Lwt.return (Some (Plain cl_fd, cl_sa))
  | SSL(ctx,_,socket) ->
     let fd = Lwt_ssl.get_fd socket in
     Lwt_unix.accept fd >>= fun (cl_fd, cl_sa) ->
     Lwt.catch
       (fun () ->
         Typed_ssl.Lwt.ssl_accept cl_fd ctx >>= fun (x,s) ->
         let r = SSL (ctx,x,s) in
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
  | SSL (ctx,_,socket) ->
     let fd = Lwt_ssl.get_fd socket in
     Tcp_keepalive.apply fd tcp_keepalive
  | Rsocket fd -> ()

let connect fd address = match fd with
  | Plain fd   -> Lwt_unix.connect fd address
  | SSL(ctx,_,socket) ->
  (* 
    Lwt_unix.connect fd address >>= fun () ->
    Typed_ssl.Lwt.ssl_connect fd ctx >>= fun lwt_s -> 
    let r = Net_fd.wrap_ssl lwt_s in 
   *)
    failwith "ssl.connect"
  | Rsocket fd ->
     Lwt_log.debug_f "Rsocket.connect"
     >>= fun () ->
     Lwt_rsocket.connect fd address

let close = function
  | Plain socket    -> Lwt_unix.close socket
  | SSL (_, _, socket) -> Lwt_ssl.close socket
  | Rsocket socket  -> Lwt_rsocket.close socket

let to_connection ~in_buffer ~out_buffer = function
  | Plain fd ->
     let ic = Lwt_io.of_fd ~buffer:in_buffer ~mode:Lwt_io.input fd
     and oc = Lwt_io.of_fd ~buffer:out_buffer ~mode:Lwt_io.output fd in
     (ic, oc)
  | SSL (_, _, socket) ->
     let ic = Lwt_ssl.in_channel_of_descr ~buffer:in_buffer socket
     and oc = Lwt_ssl.out_channel_of_descr ~buffer:out_buffer socket in
     (ic, oc)
  | Rsocket socket -> failwith "todo: to_connection (Rsocket rsocket) "

let make_ic ~buffer = function
  | Plain fd       -> Lwt_io.of_fd ~buffer ~mode:Lwt_io.input fd
  | SSL (_, _,socket) -> Lwt_ssl.in_channel_of_descr ~buffer socket
  | Rsocket socket -> failwith "make_ic rsocket"

let write_all_ssl socket bytes offset length =
  let write_from_source = Lwt_ssl.write socket bytes in
  Lwt_extra2._write_all write_from_source offset length

let write_all nfd bytes offset length = match nfd with
  | Plain fd          -> Lwt_extra2.write_all fd bytes offset length
  | SSL (_, _, socket) -> write_all_ssl socket bytes offset length
  | Rsocket socket ->
     let write_from_source offset todo =
       Lwt_rsocket.send socket bytes offset todo []
     in
     Lwt_extra2._write_all write_from_source offset length
                           
let write_all' nfd bytes = write_all nfd bytes 0 (Bytes.length bytes)

let write_all_lwt_bytes nfd bs offset length = match nfd with
  | Plain fd -> Lwt_extra2.write_all_lwt_bytes fd bs offset length
  | SSL (_, _, socket) ->
     Lwt_extra2._write_all
       (Lwt_ssl.write_bytes socket bs)
       offset length
  | Rsocket socket ->
     let write_from_source offset todo =
       Lwt_rsocket.Bytes.send socket bs offset todo []
     in
     Lwt_extra2._write_all write_from_source offset length
                           
let read_all nfd target offset length = match nfd with
  | Plain fd -> Lwt_extra2.read_all fd target offset length
  | SSL (_, _,socket) ->
     let read_to_target = Lwt_ssl.read socket target in
     Lwt_extra2._read_all read_to_target offset length
  | Rsocket socket ->
     Lwt_log.debug_f "Rsocket.read_all _ _ %i %i" offset length >>= fun () ->
     let read_to_target offset todo =
       Lwt_log.debug_f "Rsocket.recv %i %i" offset todo >>= fun () ->
       Lwt_rsocket.recv socket target offset todo [] >>= fun read ->
       Lwt_log.debug_f "Rsocket.recv %i" read >>= fun () ->
       Lwt.return read
     in
     Lwt_extra2._read_all read_to_target offset length

let read_all_exact nfd target offset length =
  read_all nfd target offset length
  >>= Lwt_extra2.expect_exact_length length

let read_all_lwt_bytes_exact nfd target offset length = match nfd with
  | Plain fd -> Lwt_extra2.read_all_lwt_bytes_exact fd target offset length
  | SSL (_, _, socket) ->
     Lwt_extra2._read_all
       (Lwt_ssl.read_bytes socket target)
       offset length
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
     Lwt_extra2._read_all read_to_target offset length
     >>= Lwt_extra2.expect_exact_length length

let cork = function
  | Plain fd ->
     Lwt_unix.setsockopt fd Lwt_unix.TCP_NODELAY false 
  | SSL (_,_, socket) ->
     Lwt_unix.setsockopt
       (Lwt_ssl.get_fd socket)
       Lwt_unix.TCP_NODELAY false 
  | Rsocket fd  ->
     Lwt_rsocket.setsockopt fd Lwt_unix.TCP_NODELAY false    

let uncork = function
  | Plain fd   ->
     Lwt_unix.setsockopt fd Lwt_unix.TCP_NODELAY true
  | SSL (_,_,socket) ->
     Lwt_unix.setsockopt
       (Lwt_ssl.get_fd socket)
       Lwt_unix.TCP_NODELAY true
  | Rsocket socket ->
     Lwt_rsocket.setsockopt socket Lwt_unix.TCP_NODELAY true
  
      
