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

let make_address ip port =
  let ha = Unix.inet_addr_of_string ip in
  Unix.ADDR_INET (ha,port)

let string_of_address = function
  | Unix.ADDR_INET(addr, port) ->
     Printf.sprintf
       "ADDR_INET(%s,%i)"
       (Unix.string_of_inet_addr addr) port
  | Unix.ADDR_UNIX s -> Printf.sprintf "ADDR_UNIX %s" s

exception ConnectTimeout

let connect_with ip port transport ~tls_config =

  let address = make_address ip port in
  let fd =
    Net_fd.socket
      (Unix.domain_of_sockaddr address) Unix.SOCK_STREAM 0
      transport
  in

  let (fdi:int) = Net_fd.identifier fd in
  Lwt_log.debug_f
    "connect_with : %s %i %s fd:%i" ip port ([%show: Tls.t option] tls_config) fdi
  >>= fun () ->
  let closer () =
    Lwt_log.debug_f "closing fd:%i" fdi >>= fun () ->
    Net_fd.close fd
  in
  match Tls.to_client_context tls_config with
  | None ->
     Lwt.catch
       (fun () ->
        Lwt.pick
          [ (Lwt_unix.sleep 1. >>= fun () ->
             Lwt_log.debug_f
               "timeout while connecting to fd=%i ip=%s port=%i"
               fdi ip port >>= fun () ->
             Lwt.fail ConnectTimeout);
            (Net_fd.connect fd address >>= fun () ->
             Lwt.return (fd , closer)); ]
       )
       (fun exn ->
        closer () >>= fun () ->
        Lwt.fail exn)
  | Some ctx ->
     begin
       Lwt.catch
         (fun () ->
          Lwt_extra2.with_timeout
            1.
            ~msg:(Printf.sprintf
                    "timeout while connecting to fd=%i ip=%s port=%i (ssl)"
                    fdi ip port)
            (fun () ->
             Net_fd.connect fd address >>= fun () ->
             (* Typed_ssl.Lwt.ssl_connect fd ctx >>= fun lwt_s ->
             let r = Net_fd.wrap_ssl lwt_s in *)
             let () = failwith "todo:ssl_connect" in
             Lwt.return (fd, closer))
         )
         (fun exn ->
          closer () >>= fun () ->
          begin
            match exn with
              | Ssl.Connection_error e ->
                 Lwt_log.debug_f ~exn "e:%S" (Ssl.get_error_string ())
              | _ -> Lwt.return_unit
          end
          >>= fun () ->
          Lwt.fail exn)
     end

let with_connection ip port transport ~tls_config ~buffer_pool f =
  connect_with ip port transport ~tls_config >>= fun(nfd, closer) ->
  let in_buffer = Buffer_pool.get_buffer buffer_pool in
  let out_buffer = Buffer_pool.get_buffer buffer_pool in
  let conn = Net_fd.to_connection ~in_buffer ~out_buffer nfd in
  Lwt.finalize
    (fun () -> f conn)
    (fun () ->
     Lwt.finalize
       closer
       (fun () ->
        Buffer_pool.return_buffer buffer_pool in_buffer;
        Buffer_pool.return_buffer buffer_pool out_buffer;
        Lwt.return ()))

type conn_info = {
    ips:string list;
    port: int;
    transport : Net_fd.transport;
    tls_config: Tls.t option
  } [@@deriving show]

let make_conn_info ips port ?(transport=Net_fd.TCP) tls_config = {ips;port;transport;tls_config}

exception No_connection

let first_connection ~conn_info =
  Lwt_log.debug_f
    "connecting to %s" (show_conn_info conn_info) >>= fun () ->
  let count = List.length conn_info.ips in
  let res = Lwt_mvar.create_empty () in
  let err = Lwt_mvar.create None in
  let l = Lwt_mutex.create () in
  let cd = Lwt_extra2.CountDownLatch.create ~count in
  let port = conn_info.port in
  let tls_config = conn_info.tls_config in
  let transport = conn_info.transport in
  let f' ip =
    Lwt.catch
      (fun () ->
         connect_with ip port transport ~tls_config >>= fun (fd, closer) ->
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
  let ts = List.map f' conn_info.ips in
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

let first_connection' ?close_msg buffer_pool ~conn_info =
  first_connection ~conn_info >>= fun (nfd, closer) ->
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


let make_server
      ?(cancel = Lwt_condition.create ())
      ?(server_name = "server")
      ?max
      hosts port ~transport
      ~tcp_keepalive
      ~ctx protocol
  =
  let count = ref 0 in
  let allow_connection =
    match max with
    | None -> fun () -> true
    | Some max -> fun () -> max > !count
  in
  let server_loop socket_address =
    let rec inner (listening_socket:Net_fd.t) =
      Lwt.pick
        [ Net_fd.accept listening_socket;
          (Lwt_condition.wait cancel >>= fun () ->
           Lwt.fail Lwt.Canceled); ]
      >>= fun cl_fdo ->
      let () =
        match cl_fdo with
        | None -> ()
        | Some (cl_fd, cl_sa) ->
             Lwt.ignore_result
               begin
                 Lwt.finalize
                   (fun () ->
                    let () = incr count in
                    if allow_connection ()
                    then
                      Lwt.catch
                        (fun () ->
                          Net_fd.apply_keepalive tcp_keepalive cl_fd; 
                          Lwt_log.info_f "%s: new client connection" server_name >>= fun () ->
                          protocol cl_fd)
                        (function
                          | End_of_file ->
                             Lwt_log.debug_f "%s: End_of_file from client" server_name
                          | exn ->
                             Lwt_log.info_f
                               "%s: exception occurred in client connection: %s"
                               server_name
                               (Printexc.to_string exn)
                        )
                    else
                      (Lwt_log.warning_f "Denying connection, too many client connections %i" !count))
                   (fun () ->
                    let () = decr count in
                    Lwt.catch
                      (fun () -> Net_fd.close cl_fd)
                      (fun exn ->
                       Lwt_log.debug_f
                         "%s: exception occurred during close of client connection: %s"
                         server_name
                         (Printexc.to_string exn)
                      )
                   )
               end
      in
      inner listening_socket
    in
    let domain = Unix.domain_of_sockaddr socket_address in
    let listening_socket = Net_fd.socket domain Unix.SOCK_STREAM 0 transport in
    Lwt.finalize
      (fun () ->
        Net_fd.setsockopt listening_socket Unix.SO_REUSEADDR true;
        Net_fd.bind listening_socket socket_address;
        Net_fd.listen listening_socket 1024;
        inner listening_socket)
      (fun () ->
       Lwt_log.info_f "Closing listening socket on port %i" port >>= fun () ->
       Net_fd.close listening_socket)
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
  | ConnectTimeout
  | Unix.Unix_error(Unix.ECONNREFUSED, "connect", "")
  | Failure "no connection" -> true
  | _ -> false
