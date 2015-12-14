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

open Lwt
open Prelude

module Pool = struct
  module Albamgr = struct
    open Albamgr_protocol.Protocol
    type t = (Albamgr_client.single_connection_client *
              (unit -> unit Lwt.t)) Lwt_pool2.t

    let make ~size cfg tls_config buffer_pool =
      let factory () =
        let ccfg = Arakoon_config.to_arakoon_client_cfg tls_config !cfg in
        let tls = Tls.to_client_context tls_config in
        let open Albamgr_client in
        Lwt.catch
          (fun () -> make_client buffer_pool ccfg)
          (let open Client_helper in
           let open MasterLookupResult in
           function
            | Arakoon_exc.Exception(Arakoon_exc.E_NOT_MASTER, _master)
            | Error (Unknown_node (_master, (_, _))) ->
               begin
                 retrieve_cfg_from_any_node ~tls !cfg >>= fun cfg' ->
                 match cfg' with
                 | Res cfg' ->
                    let () = cfg := cfg' in
                    make_client
                      buffer_pool
                      (Arakoon_config.to_arakoon_client_cfg tls_config !cfg)
                 | Retry -> Lwt.fail_with "retry later"
               end
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
              ~factory:(fun () ->
                 t.get_nsm_host_config nsm_host_id
                 >>= fun nsm ->
                 match nsm.Nsm_host.kind with
                 | Nsm_host.Arakoon cfg ->
                    let ccfg = Arakoon_config.to_arakoon_client_cfg t.tls_config cfg in
                    Nsm_host_client.make_client
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

  module Osd = struct
    type osd = Osd.osd
    open Albamgr_protocol.Protocol
    open Nsm_model
    type osd_pool = (osd * (unit -> unit Lwt.t)) Lwt_pool2.t

    type t = {
      pools : (Osd.id, osd_pool) Hashtbl.t;
      get_osd_kind : Osd.id -> OsdInfo.kind Lwt.t;
      pool_size : int;
      buffer_pool : Buffer_pool.t;
      tls_config: Tls.t option;
    }

    let make ~size ~get_osd_kind buffer_pool tls_config =
      let pools = Hashtbl.create 0 in
      { pools;
        get_osd_kind;
        pool_size = size;
        buffer_pool;
        tls_config
      }

    let factory tls_config buffer_pool =
      function
      | OsdInfo.Asd (conn_info', asd_id) ->
         let () =
           Lwt_log.ign_debug_f
             "factory: conn_info':%s"
             ([%show :Nsm_model.OsdInfo.conn_info] conn_info')
         in
         let conn_info = Asd_client.conn_info_from ~tls_config conn_info' in

         Asd_client.make_client buffer_pool ~conn_info (Some asd_id)
         >>= fun (asd, closer) ->
         let osd = new Asd_client.asd_osd asd_id asd in
         Lwt.return (osd, closer)
      | OsdInfo.Kinetic (conn_info', kinetic_id) ->
         let conn_info = Asd_client.conn_info_from ~tls_config conn_info' in
         Kinetic_client.make_client buffer_pool ~conn_info kinetic_id

    let use_osd t ~(osd_id:int32) f =
      let get_pool () =
        try Hashtbl.find t.pools osd_id |> Lwt.return
        with Not_found ->
          begin
            t.get_osd_kind osd_id >>= fun kind ->
            let factory () =
              factory t.tls_config t.buffer_pool kind
            in
            let p =
              Lwt_pool2.create
                t.pool_size
                ~check:(fun _ exn ->
                        (* TODO some exns shouldn't invalidate the connection *)
                        false)
                ~factory
                ~cleanup:(fun (_, closer) -> closer ())
            in
            Hashtbl.add t.pools osd_id p;
            Lwt.return p
          end
      in
      get_pool () >>= fun pool ->
      Lwt_pool2.use
        pool
        (fun p -> f (fst p))

    let invalidate t ~osd_id =
      if Hashtbl.mem t.pools osd_id
      then begin
        Lwt_pool2.finalize (Hashtbl.find t.pools osd_id) |> Lwt.ignore_result;
        Hashtbl.remove t.pools osd_id
      end

    let invalidate_all t =
      List.iter
        (fun (osd_id, _) -> invalidate t ~osd_id)
        (Hashtbl.to_assoc_list t.pools)
  end

end
