(*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*)

open Ctypes
open Prelude

let _setsockopt =
(*
    int setsockopt(int sockfd, int level, int optname,
                   const void *optval, socklen_t optlen);
*)

  Foreign.foreign "setsockopt"
    ~check_errno:true
    (int @-> int @-> int  @-> ptr void @-> int @-> returning int)

let _setsockopt' =
(*
    int setsockopt(int sockfd, int level, int optname,
                   const void *optval, socklen_t optlen);
*)

  Foreign.foreign "setsockopt"
    ~check_errno:true
    (int @-> int @-> int  @-> string @-> int @-> returning int)

let _ipproto_ip = 0
let _sol_ip = 0
let _ip_add_membership = 35
let _ip_multicast_ttl = 33

let join_my_mcast (socket:Unix.file_descr) =
  let mreq = "\xef\x01\x02\x03\x00\x00\x00\x00" in
  let (socket_i:int) = Obj.magic socket in
  _setsockopt'
    socket_i
    _ipproto_ip
    _ip_add_membership
    mreq
    8

let set_multicast_ttl (socket:Unix.file_descr) =
 (*setsockopt(3, SOL_IP, IP_MULTICAST_TTL, [1], 4) = 0 *)
 let (socket_i:int) = Obj.magic socket in
 let optval = allocate int 255 in
 _setsockopt
   socket_i
   _sol_ip
   _ip_multicast_ttl
   (to_voidp optval)
   (sizeof int)

type extra_info =
  { node_id : string;
    version: string;
    total: int64;
    used: int64;
  }[@@deriving show]

type record = {
    extras: extra_info option;
    ips: string list;
    port :int option;
    tlsPort: int option;
    useRdma: bool;
    id : string;
  }[@@deriving show]

type t =
  | Good of string * record
  | Bad of string

let parse s addr0 =
  let open Tiny_json in
  try
    let r = Json.parse s in
    let get_f n = Json.getf n r in
    let id, extras =
      try
        let version = Json.as_string (get_f "version") in
        let used =  Int64.of_string (Json.as_string (get_f "used_bytes")) in
        let total = Int64.of_string (Json.as_string (get_f "total_bytes")) in
        let node_id = Json.as_string (get_f "node_id") in
        let id = Json.as_string (get_f "id") in
        id, Some {node_id;version;used;total;}
      with | Json.JSON_InvalidField _ ->
        let id = Json.as_string (get_f "world_wide_name") in
        id, None
    in
    let nics = Json.as_list (get_f "network_interfaces") in
    let get_option_as convertor name =
      try Some (convertor (get_f name))
      with | Json.JSON_InvalidField _ -> None
    in
    let port    = get_option_as Json.as_int "port" in
    let tlsPort = get_option_as Json.as_int "tlsPort" in
    let useRdma =
      try get_f "useRdma" |> Json.as_bool
      with | Json.JSON_InvalidField _ -> false
    in
    let set0 = StringSet.of_list addr0 in
    let ip_set =
      if nics = []
      then set0
      else
        List.fold_left
          (fun acc nic ->
             let ipv4_j = Json.getf "ipv4_addr" nic in
             let ipv4 = Json.as_string ipv4_j in
             if ipv4 = "127.0.0.1"
             then acc
             else StringSet.add ipv4 acc
          ) StringSet.empty nics
    in
    let ips = StringSet.elements ip_set in

    Good (s, { id; extras; ips; port; tlsPort; useRdma })
  with
  | exn ->
     let () = Lwt_log.ign_debug_f ~exn "ex => Bad" in
     Bad s

open Lwt.Infix

let discovery seen =
  let min_delay = 0.1 in
  let delay = ref min_delay in
  let rec outer () =
    Lwt.catch
      (fun () ->
       let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
       let close_socket () = Lwt_extra2.ignore_errors  (fun () -> Lwt_unix.close socket) in
       let use_socket () =
         Lwt_unix.setsockopt socket Unix.SO_REUSEADDR true;
         let sa = Unix.ADDR_INET (Unix.inet_addr_any, 8123) in
         let _ = join_my_mcast (Lwt_unix.unix_file_descr socket) in
         Lwt_unix.Versioned.bind_2 socket sa >>= fun () ->
         let maxlen = 16384 in
         let buffer = Bytes.create maxlen in
         let () = delay := min_delay in
         let rec loop () =
           Lwt_unix.with_timeout 20.0 (fun() -> Lwt_unix.recvfrom socket buffer 0 maxlen [])
           >>= fun (len, portaddr) ->
           let addr0 =
             match portaddr with
             | Unix.ADDR_INET (inet_addr ,p) ->
                [(Unix.string_of_inet_addr inet_addr)]
             | _ -> []
           in
           let got = Bytes.sub buffer 0 len in
           Lwt_log.debug_f "got:\n%s\n" got >>= fun () ->
           let record = parse got addr0 in
           Lwt.async
             (fun () ->
              Lwt_extra2.ignore_errors
                (fun () -> seen record));
           loop ()
         in
         loop ()
       in
       Lwt.finalize use_socket close_socket
      )
      (fun exn ->
       Lwt_log.warning_f ~exn "Exception in discovery loop. retrying in %2.0f" !delay >>= fun () ->
       Lwt_unix.sleep !delay)
    >>= fun () ->
    let delay' = min (!delay *. 2.0) 30.0 in
    let () = delay := delay' in
    outer ()
  in
  outer ()

let multicast
      (id:string) (node_id:string) ips
      ~(port:int option)
      ~(tlsPort:int option)
      ~(useRdma: bool option)
      period
      ~(disk_usage:unit -> (int64 * int64) Lwt.t)
  =
  Lwt_log.debug_f "multicast: .... useRdma:%s" ([%show :bool option] useRdma) >>= fun () ->
  let data (used:int64) (total:int64) =
    let b = Buffer.create 128 in
    let add_s s = Buffer.add_string b s in
    let add_ip ip =
      add_s "{\"ipv4_addr\": \"";
      add_s ip;
      add_s "\"}"
    in
    let rec add_ips = function
      | [] -> ()
      | [ip0] -> add_ip ip0
      | ip0 :: ip1 :: ips ->
         begin
           add_ip ip0;
           add_s ", ";
           add_ips (ip1 :: ips)
         end
    in
    let add_pair name s =
      add_s "\"";
      add_s name;
      add_s "\" : \"";
      add_s s;
      add_s "\"";
    in
    let maybe_add_int name = function
      | None -> ()
      | Some i ->
         begin
           add_s ", \"";
           add_s name;
           add_s "\" : ";
           add_s (Printf.sprintf "%i" i)
         end
    in
    let maybe_add_bool name = function
      | None -> ()
      | Some b ->
         begin
           add_s ", \"";
           add_s name;
           add_s "\" : ";
           add_s (string_of_bool b)
         end
    in
    add_s "{ ";
    add_pair "id" id;
    add_s ", ";
    add_pair "node_id" node_id;
    maybe_add_int "port" port;
    maybe_add_int "tlsPort" tlsPort;
    maybe_add_bool "useRdma" useRdma;
    add_s ", ";
    add_pair "used_bytes" (Printf.sprintf "%Li" used);
    add_s ", ";
    add_pair "total_bytes"(Printf.sprintf "%Li" total);
    add_s ", ";
    add_pair "version" "AsdV1";
    add_s ", \"network_interfaces\":[";
    add_ips ips;
    add_s " ]}";
    Buffer.contents b
  in

  let group = "239.1.2.3" in
  let sa = Unix.ADDR_INET (Unix.inet_addr_of_string group, 8123) in

  let rec outer () =
    Lwt.catch
      (fun () ->
       let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
       let close_socket () = Lwt_extra2.ignore_errors  (fun () -> Lwt_unix.close socket) in
       let use_socket () =
         let _ : int = set_multicast_ttl (Lwt_unix.unix_file_descr socket) in
         let rec inner () =
           Lwt.catch
             disk_usage
             (fun exn ->
              Lwt_log.error
                ~exn
                "Exception while getting disk usage, exiting process" >>= fun () ->
              exit 1) >>= fun (used, cap) ->
           let msg = data used cap in
           Lwt_unix.sendto socket msg 0 (String.length msg) [] sa >>= fun _ ->
           Lwt_unix.sleep period >>=
             inner
         in
         inner ()
       in
       Lwt.finalize use_socket close_socket
      )
      (function
        | Unix.Unix_error _ as exn ->
          Lwt_log.warning_f ~exn "Ignoring exception during broadcast loop"
        | exn -> Lwt.fail exn) >>= fun () ->
    Lwt_unix.sleep period >>=
    outer
  in
  outer ()



let get_kind buffer_pool (conn_info:Networking2.conn_info) =
  Lwt_log.debug_f "get_kind conn_info:%s" (Networking2.show_conn_info conn_info) >>= fun () ->
  let conn_info' =
    let open Networking2 in
    let use_tls = match conn_info.tls_config with
      | None   -> false
      | Some _ -> true
    in
    let use_rdma =
      match conn_info.transport with
      | Net_fd.TCP  -> false
      | Net_fd.RDMA -> true
    in
    (conn_info.ips, conn_info.port, use_tls, use_rdma)
  in
  let maybe_kinetic conn_info =
    Lwt_log.debug "is it a kinetic?" >>= fun () ->
    Lwt.catch
      (fun () ->
       Networking2.first_connection' buffer_pool ~conn_info
       >>= fun (fd, conn, closer) ->
       Lwt.finalize
           (fun () ->
            Lwt_unix.with_timeout
              0.5 (* messages comes swiftly *)
              (fun () ->
               let secret = "asdfasdf" in
               let cluster_version = 0L in
               let open Kinetic in
               Kinetic.handshake secret cluster_version conn >>= fun session ->
               let config = Kinetic.get_config session in
               let open Config in
               let wwn = config.world_wide_name in
               Lwt_io.printlf "wwn:%s" wwn>>= fun () ->
               let long_id = wwn in
               let kind = Nsm_model.OsdInfo.Kinetic(conn_info',long_id) in
               let r = Some kind in
               Lwt.return r)
           )
           closer
      )
      (fun exn ->
       Lwt_log.debug_f ~exn "probably not a kinetic..." >>= fun () ->
       Lwt.return None
      )
  in
  let maybe_asd conn_info =
    Lwt_log.debug "is it an asd?" >>= fun () ->
    Lwt.catch
      (fun () ->
       Asd_client.make_client ~conn_info (Some "xxx")
       >>= fun (client, closer) ->
       Lwt.finalize
         (fun () -> Lwt.return None)
         (fun () -> closer ())
      )
      (function
        | Asd_client.BadLongId(_,long_id) ->
           let kind = Nsm_model.OsdInfo.Asd(conn_info', long_id) in
           Lwt.return (Some kind)
        | exn ->
           Lwt_log.debug_f "probably not an asd..." ~exn
           >>= fun () ->
           Lwt.return None
      )

  in
  let rec loop_kinds xo ms =
    match xo,ms with
    | Some x, _ -> Lwt.return xo
    | None, m :: ms ->
       begin
         m conn_info >>= fun xo ->
         loop_kinds xo ms
       end
    | None  ,[] -> Lwt.return None
  in
  let rec attempt i f xo =
    Lwt_log.debug_f "attempt:i=%i" i >>= fun () ->
    match i,xo with
    |0,_      -> Lwt.return xo
    |_,Some _ -> Lwt.return xo
    |i, None  -> loop_kinds None [maybe_asd; maybe_kinetic] >>= fun xo ->
                 Lwt_log.debug_f "sleeping for %f" f >>= fun () ->
                 Lwt_unix.sleep f >>= fun () ->
                 attempt (i-1) (f *. 2.0) xo
  in
  attempt 5 0.2 None
