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

let create_connection ?in_buffer ?out_buffer ip port =
  let address = make_address ip port in
  let fd =
    Lwt_unix.socket
      (Unix.domain_of_sockaddr address)
      Unix.SOCK_STREAM 0 in
  Lwt_io.open_connection ~fd ?in_buffer ?out_buffer address
  >>= fun conn ->
  Lwt.return (fd, conn)


let closer (ic,oc) () =
  Lwt_extra2.ignore_errors
    (fun () ->
       Lwt_io.close ic >>= fun () ->
       Lwt_io.close oc)

exception No_connection

let first_connection
      ?(buffer_size=Lwt_io.default_buffer_size ())
      ips port =
  let count = List.length ips in
  let res = Lwt_mvar.create_empty () in
  let err = Lwt_mvar.create None in
  let l = Lwt_mutex.create () in
  let cd = Lwt_extra2.CountDownLatch.create ~count in
  let f' ip =
    Lwt.catch
      (fun () ->
         create_connection
           ~in_buffer:(Lwt_bytes.create buffer_size)
           ~out_buffer:(Lwt_bytes.create buffer_size)
           ip port
         >>= fun (fd, conn) ->
         if Lwt_mutex.is_locked l
         then closer conn ()
         else begin
           Lwt_mutex.lock l >>= fun () ->
           Lwt_mvar.put res (`Success (fd, conn))
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


let make_server hosts port protocol =
  let server_loop socket_address =
    let domain = Unix.domain_of_sockaddr socket_address in
    let listening_socket = Lwt_unix.socket domain Unix.SOCK_STREAM 0 in
    Lwt_unix.setsockopt listening_socket Unix.SO_REUSEADDR true;
    Lwt_unix.bind listening_socket socket_address;
    Lwt_unix.listen listening_socket 1024;
    let rec inner () =
      Lwt_unix.accept listening_socket >>= fun (fd, cl_socket_address) ->
      Lwt_log.info "Got new client connection" >>= fun () ->
      Lwt.ignore_result
        begin
          Lwt.finalize
            (fun () ->
               Lwt.catch
                 (fun () -> protocol fd)
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
                 (fun () -> Lwt_unix.close fd)
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
