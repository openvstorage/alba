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

open Lwt
open Prelude

module Pool = struct
  module Albamgr = struct
    open Albamgr_protocol.Protocol
    type t = (Albamgr_client.single_connection_client *
              (unit -> unit Lwt.t)) Lwt_pool2.t

    let make ~size cfg buffer_pool =
      let factory () =
        let ccfg = Arakoon_config.to_arakoon_client_cfg !cfg in
        Lwt.catch
          (fun () -> Albamgr_client.make_client buffer_pool ccfg)
          (let open Client_helper in
           let open MasterLookupResult in
           function
            | Error (Unknown_node (_master, (node', cfg'))) ->
              let cluster_id = fst !cfg in
              with_client'
                ~tls:None
                cfg' cluster_id
                (fun arakoon ->
                   Albamgr_client.wrap_around' arakoon >>= fun mgr ->
                   mgr # get_client_config >>= fun cfg' ->
                   cfg := cfg';
                   Lwt.return ()) >>= fun () ->
              Albamgr_client.make_client
                buffer_pool
                (Arakoon_config.to_arakoon_client_cfg !cfg)
            | exn ->
              Lwt.fail exn)
        >>= fun (c, node_name, closer) ->
        Lwt.return (c, closer)
      in
      Lwt_pool2.create
        size
        ~check:(fun _ exn ->
            (* TODO some exns shouldn't invalidate the connection *)
            false)
        ~factory
        ~cleanup:(fun (_, closer) -> closer ())

    let use_mgr t f =
      Lwt_log.debug_f "Taking an albamgr from the connection pool" >>= fun () ->
      Lwt_pool2.use
        t
        (fun p ->
           Lwt_log.debug_f "Got an albamgr from the connection pool" >>= fun () ->
           f (fst p))

  end

  module Nsm_host = struct
    open Albamgr_protocol.Protocol
    type nsm_pool =
        (Nsm_host_client.single_connection_client *
         (unit -> unit Lwt.t)) Lwt_pool2.t
    type t = {
      get_nsm_host_config : Nsm_host.id -> Nsm_host.t Lwt.t;
      pools : (Nsm_host.id, nsm_pool) Hashtbl.t;
      pool_size : int;
      buffer_pool : Buffer_pool.t;
    }

    let make ~size get_nsm_host_config buffer_pool =
      let pools = Hashtbl.create 0 in
      { get_nsm_host_config;
        pools;
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
              ~factory:(fun () ->
                 t.get_nsm_host_config nsm_host_id
                 >>= fun nsm ->
                 match nsm.Nsm_host.kind with
                 | Nsm_host.Arakoon cfg ->
                    Nsm_host_client.make_client
                      t.buffer_pool
                      (Arakoon_config.to_arakoon_client_cfg cfg))
              ~cleanup:(fun (_, closer) -> closer ())
          in
          Hashtbl.add t.pools nsm_host_id p;
          p
      in
      Lwt_log.debug_f
        "Taking an nsm host client for %S from the connection pool"
        nsm_host_id >>= fun () ->
      Lwt_pool2.use
        pool
        (fun p ->
           Lwt_log.debug_f
             "Got an nsm host client for %S from the connection pool"
             nsm_host_id >>= fun () ->
           f (fst p))

    let invalidate t ~nsm_host_id =
      if Hashtbl.mem t.pools nsm_host_id
      then begin
        Lwt_pool2.finalize (Hashtbl.find t.pools nsm_host_id);
        Hashtbl.remove t.pools nsm_host_id
      end
  end

  module Osd = struct
    type osd = Osd.osd
    open Albamgr_protocol.Protocol

    type osd_pool = (osd * (unit -> unit Lwt.t)) Lwt_pool2.t

    type t = {
      pools : (Osd.id, osd_pool) Hashtbl.t;
      get_osd_kind : Osd.id -> Osd.kind Lwt.t;
      pool_size : int;
      buffer_pool : Buffer_pool.t;
    }

    let make ~size get_osd_kind buffer_pool =
      let pools = Hashtbl.create 0 in
      { pools;
        get_osd_kind;
        pool_size = size;
        buffer_pool;
      }

    let factory buffer_pool =
      let open Osd in
      function
      | Asd (ips, port, asd_id) ->
        Asd_client.make_client buffer_pool ips port (Some asd_id)
        >>= fun (asd, closer) ->
        let osd = new Asd_client.asd_osd asd_id asd in
        Lwt.return (osd, closer)
      | Kinetic (ips, port, kinetic_id) ->
        Kinetic_client.make_client buffer_pool ips port kinetic_id
        >>= fun (kin, closer) ->
        Lwt.return (kin, closer)

    let use_osd t ~osd_id f =
      let pool =
        try Hashtbl.find t.pools osd_id
        with Not_found ->
          let p =
            Lwt_pool2.create
              t.pool_size
              ~check:(fun _ exn ->
                  (* TODO some exns shouldn't invalidate the connection *)
                  false)
              ~factory:(fun () -> t.get_osd_kind osd_id >>= factory t.buffer_pool)
              ~cleanup:(fun (_, closer) -> closer ())
          in
          Hashtbl.add t.pools osd_id p;
          p
      in
      Lwt_pool2.use
        pool
        (fun p -> f (fst p))

    let invalidate t ~osd_id =
      if Hashtbl.mem t.pools osd_id
      then begin
        Lwt_pool2.finalize (Hashtbl.find t.pools osd_id);
        Hashtbl.remove t.pools osd_id
      end
  end

end
