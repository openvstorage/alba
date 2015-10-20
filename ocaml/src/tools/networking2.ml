(*
Copyright 2015 Open vStorage NV

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

let make_address ip port =
  let ha = Unix.inet_addr_of_string ip in
  Unix.ADDR_INET (ha,port)

let string_of_address = function
  | Unix.ADDR_INET(addr, port) ->
     Printf.sprintf
       "ADDR_INET(%s,%i)"
       (Unix.string_of_inet_addr addr) port
  | Unix.ADDR_UNIX s -> Printf.sprintf "ADDR_UNIX %s" s

let connect_with ip port =
  let address = make_address ip port in
  let fd =
    Lwt_unix.socket
      (Unix.domain_of_sockaddr address)
      Unix.SOCK_STREAM 0 in
  let r = Net_fd.wrap fd in
  Lwt.catch
    (fun () ->
     Lwt_unix.connect fd address >>= fun () ->
     Lwt.return (r , fun () -> Lwt_unix.close fd))
    (fun exn ->
     Lwt_unix.close fd >>= fun () ->
     Lwt.fail exn)

exception No_connection

let first_connection ips port =
  let count = List.length ips in
  let res = Lwt_mvar.create_empty () in
  let err = Lwt_mvar.create None in
  let l = Lwt_mutex.create () in
  let cd = Lwt_extra2.CountDownLatch.create ~count in
  let f' ip =
    Lwt.catch
      (fun () ->
         connect_with ip port >>= fun (fd, closer) ->
         if Lwt_mutex.is_locked l
         then closer ()
         else begin
           Lwt_mutex.lock l >>= fun () ->
           Lwt_mvar.put res (`Success (fd, closer))
         end)
      (fun exn ->
         Lwt.protected (
           Lwt_log.debug_f ~exn "Failed to connect to %s:%i" ip port >>= fun () ->
           Lwt_mvar.take err >>= begin function
             | Some _ as v -> Lwt_mvar.put err v
             | None -> Lwt_mvar.put err (Some exn)
           end >>= fun () ->
           Lwt_extra2.CountDownLatch.count_down cd;
           Lwt.return ()) >>= fun () ->
         Lwt.fail exn)
  in
  let ts = List.map f' ips in
  Lwt.finalize
    (fun () ->
       Lwt.pick [
         (Lwt_mvar.take res >>= begin function
             | `Success v -> Lwt.return v
             | `Failure exn -> Lwt.fail exn
           end);
         (Lwt_extra2.CountDownLatch.await cd >>= fun () ->
          Lwt_mvar.take err >>= function
          | None -> Lwt.fail No_connection
          | Some e -> Lwt.fail e)
       ]
    )
    (fun () ->
       Lwt_list.iter_p (fun t ->
           let () = try
               Lwt.cancel t
             with _ -> () in
           Lwt.return ())
         ts)

let to_connection ~in_buffer ~out_buffer fd =
  let ic = Lwt_io.of_fd ~buffer:in_buffer ~mode:Lwt_io.input fd
  and oc = Lwt_io.of_fd ~buffer:out_buffer ~mode:Lwt_io.output fd in
  (ic,oc)

let first_connection' ?close_msg buffer_pool ips port =
  first_connection ips port >>= fun (nfd, closer) ->
  let in_buffer = Buffer_pool.get_buffer buffer_pool in
  let out_buffer = Buffer_pool.get_buffer buffer_pool in
  let conn = Net_fd.to_connection nfd ~in_buffer ~out_buffer in
  let closer () =
    (match close_msg with
     | None -> Lwt.return ()
     | Some msg -> Lwt_log.debug msg) >>= fun () ->
    Buffer_pool.return_buffer buffer_pool in_buffer;
    Buffer_pool.return_buffer buffer_pool out_buffer;
    closer ()
  in
  Lwt.return (nfd, conn, closer)

let make_server hosts port protocol =
  let server_loop socket_address =
    let domain = Unix.domain_of_sockaddr socket_address in
    let listening_socket = Lwt_unix.socket domain Unix.SOCK_STREAM 0 in
    Lwt_unix.setsockopt listening_socket Unix.SO_REUSEADDR true;
    Lwt_unix.bind listening_socket socket_address;
    Lwt_unix.listen listening_socket 1024;
    let rec inner () =
      Lwt_unix.accept listening_socket >>= fun (_fd, cl_socket_address) ->
      let nfd = Net_fd.wrap _fd in
      Lwt_log.info "Got new client connection" >>= fun () ->
      Lwt.ignore_result
        begin
          Lwt.finalize
            (fun () ->
               Lwt.catch
                 (fun () -> protocol nfd)
                 (function
                   | End_of_file ->
                     Lwt_log.debug_f "End_of_file from client connection"
                   | exn ->
                     Lwt_log.info_f
                       "exception occurred in client connection: %s"
                       (Printexc.to_string exn)
                 ))
            (fun () ->
               Lwt.catch
                 (fun () -> Net_fd.close nfd)
                 (fun exn ->
                    Lwt_log.debug_f
                      "exception occurred during close of client connection: %s"
                      (Printexc.to_string exn)
                 ))
        end;
      inner () in
    inner ()
  in
  let addresses =
    List.map
      (fun addr -> Unix.ADDR_INET (addr, port))
      (if hosts = []
       then [ Unix.inet6_addr_any ]
       else
         List.map
           (fun host -> Unix.inet_addr_of_string host)
           hosts)
  in
  let addr_sl = List.map string_of_address addresses in
  let addr_ss = String.concat ";" addr_sl in
  Lwt_log.debug_f "addresses: [%s]%!" addr_ss
  >>= fun () ->
  Lwt.catch
    (fun () ->
       Lwt.pick (List.map server_loop addresses))
    (fun exn ->
       Lwt_log.info_f "server for %s going down: %s"
                      addr_ss
                      (Printexc.to_string exn)
       >>= fun () ->
       Lwt.fail exn)

let is_connection_failure_exn = function
  | Unix.Unix_error(Unix.ECONNREFUSED, "connect", "")
  | Failure "no connection" -> true
  | _ -> false
