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

open Lwt.Infix
open Prelude

module Pool = struct
  module Albamgr = struct
    type t = (Albamgr_client.single_connection_client *
                (unit -> unit Lwt.t)) Lwt_pool2.t

    let make ~size cfg tls_config ~tcp_keepalive buffer_pool =
      let factory () =
        let open Albamgr_client in
        Lwt.catch
          (fun () -> make_client tls_config buffer_pool !cfg)
          (let open Client_helper in
           let open MasterLookupResult in
           function
            | Arakoon_exc.Exception(Arakoon_exc.E_NOT_MASTER, _master)
            | Error (Unknown_node (_master, (_, _))) ->
               begin
                 retrieve_cfg_from_any_node ~tls_config !cfg >>= fun cfg' ->
                 match cfg' with
                 | Res cfg' ->
                    let () = cfg := cfg' in
                    (* TODO tls *)
                    make_client
                      tls_config
                      buffer_pool
                      cfg'
                 | Retry -> Lwt.fail_with "retry later"
               end
            | exn -> Lwt.fail exn
          )
        >>= fun (c, node_name, closer) ->
        Lwt.return (c, closer)
      in
      Lwt_pool2.create
        size
        ~check:(fun _ exn ->
          (* TODO some exns shouldn't invalidate the connection *)
          Lwt_log.ign_debug_f ~exn "Throwing an abm connection away after an exception";
          false)
        ~factory
        ~cleanup:(fun (_, closer) -> closer ())

    let use_mgr t f = Lwt_pool2.use t (fun p -> f (fst p))

  end

  module Nsm_host = struct
    open Albamgr_protocol.Protocol
    type nsm_pool =
        (Nsm_host_client.single_connection_client *
           (unit -> unit Lwt.t)) Lwt_pool2.t

    type t = {
      get_nsm_host_config : Nsm_host.id -> Nsm_host.t Lwt.t;
      pools : (Nsm_host.id, nsm_pool) Hashtbl.t;
      tls_config : Tls.t option;
      pool_size : int;
      buffer_pool : Buffer_pool.t;
    }

    let make ~size get_nsm_host_config ~tls_config buffer_pool =
      let pools = Hashtbl.create 0 in
      { get_nsm_host_config;
        pools;
        tls_config;
        buffer_pool;
        pool_size = size;
      }

    let use_nsm_host t ~nsm_host_id f =
      let pool =
        try Hashtbl.find t.pools nsm_host_id with
        | Not_found ->
          let p =
            Lwt_pool2.create
              t.pool_size
              ~check:(fun _ exn ->
                  (* TODO some exns shouldn't invalidate the connection *)
                  false)
              ~factory:(
                fun () ->
                t.get_nsm_host_config nsm_host_id
                >>= fun nsm ->
                match nsm.Nsm_host.kind with
                | Nsm_host.Arakoon ccfg ->
                   Nsm_host_client.make_client
                     t.tls_config
                     t.buffer_pool ccfg
              )
              ~cleanup:(fun (_, closer) -> closer ())
          in
          Hashtbl.add t.pools nsm_host_id p;
          p
      in
      Lwt_pool2.use pool (fun p -> f (fst p))

    let invalidate t ~nsm_host_id =
      if Hashtbl.mem t.pools nsm_host_id
      then begin
        Lwt_pool2.finalize (Hashtbl.find t.pools nsm_host_id) |> Lwt.ignore_result;
        Hashtbl.remove t.pools nsm_host_id
      end

    let invalidate_all t =
      List.iter
        (fun (nsm_host_id, _) -> invalidate t ~nsm_host_id)
        (Hashtbl.to_assoc_list t.pools)
  end
end
