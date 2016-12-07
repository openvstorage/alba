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

open Prelude
open Lwt.Infix
open Nsm_host_protocol
open Protocol

class type basic_client = object
  method query :  'i 'o.
                  ?consistency : Consistency.t ->
                  ('i, 'o) query -> 'i -> 'o Lwt.t

  method update : 'i 'o. ('i, 'o) update -> 'i -> 'o Lwt.t

end

class single_connection_client (ic, oc) =
  let read_response deserializer =
    Llio.input_string ic >>= fun res_s ->

    Lwt_log.debug_f
      "nsm host client read response of size %i"
      (String.length res_s)
    >>= fun () ->

    let res_buf = Llio.make_buffer res_s 0 in
    let open Nsm_model in
    match Llio.int_from res_buf with
    | 0 ->
      Lwt.return (deserializer res_buf)
    | ierr ->
      let ec = Err.int2err ierr in
      let payload = Llio.string_from res_buf in
      let exn = Err.Nsm_exn (ec, payload) in
      Lwt_log.debug_f
        "nsm host operation failed: %i %s" ierr (Err.show ec)
      >>= fun () ->
      Lwt.fail exn
  in
  let do_request tag serialize_request deserialize_response =
    let buf = Buffer.create 20 in
    Llio.int32_to buf tag;
    Lwt_log.debug_f "nsm_host_client: %s" (tag_to_name tag) >>= fun () ->
    serialize_request buf;
    Lwt_unix.with_timeout
      10.
      (fun () ->
       Lwt_extra2.llio_output_and_flush oc (Buffer.contents buf) >>= fun () ->
       read_response deserialize_response
      )
  in
  object(self :# basic_client)

    method query : type i o.
      ?consistency : Consistency.t ->
      (i, o) query -> i -> o Lwt.t =
      fun ?(consistency = Consistency.Consistent) command req ->
      do_request
        (command_to_tag (Wrap_q command))
        (fun buf ->
         Consistency.to_buffer buf consistency;
         write_query_i command buf req)
        (read_query_o command)

    method update : type i o. (i, o) update -> i -> o Lwt.t =
      fun command req ->
      do_request
        (command_to_tag (Wrap_u command))
        (fun buf -> write_update_i command buf req)
        (read_update_o command)

    method do_unknown_operation =
      let code =
        Int32.add
          100l
          (List.map
             (fun (_, code, _) -> code)
             command_map
           |> List.max
           |> Option.get_some)
      in
      Lwt.catch
        (fun () ->
         do_request
           code
           (fun buf -> ())
           (fun buf -> ())
         >>= fun () ->
         Lwt.fail_with "did not get an exception for unknown operation")
        (let open Nsm_model in
         function
          | Err.Nsm_exn (Err.Unknown_operation, _) -> Lwt.return ()
          | exn -> Lwt.fail exn)

  end

class client (client : basic_client) =
  object(self)
    val supports_deliver_messages = ref None

    method deliver_messages msgs =
      let do_new () = client # update DeliverMsgs msgs in
      let do_old () = Lwt_list.iter_s
                        (fun (msg_id, msg) ->
                         client # update
                                DeliverMsg
                                (msg, msg_id))
                        msgs in
      match !supports_deliver_messages with
      | None ->
         Lwt.catch
           (fun () -> do_new () >>= fun () ->
                      supports_deliver_messages := Some true;
                      Lwt.return_unit)
           (function
             | Nsm_model.Err.Nsm_exn (Nsm_model.Err.Unknown_operation, _) ->
                supports_deliver_messages := Some false;
                do_old ()
             | exn -> Lwt.fail exn)
      | Some true -> do_new ()
      | Some false -> do_old ()

    method cleanup_for_namespace ~namespace_id =
      client # update
        Nsm_host_protocol.Protocol.CleanupForNamespace
        namespace_id

    method get_version = client # query GetVersion ()

    method statistics (clear:bool) = client # query NSMHStatistics clear

    method nsms_query tag req =
      Lwt.catch
        (fun () -> client # query (NsmsQuery tag) req >>= fun r ->
                   Lwt.return (Some r))
        (function
          | Nsm_model.Err.Nsm_exn (Nsm_model.Err.Unknown_operation, _) ->
             Lwt.return None
          | exn ->
             Lwt.fail exn)

    method get_nsm_stats namespace_ids =
      self # nsms_query
           Nsm_protocol.Protocol.GetStats
           (List.map
              (fun id -> id, ())
              namespace_ids)

    method update_presets req =
      client # update (NsmsUpdate Nsm_protocol.Protocol.UpdatePreset) req
  end

let wrap_around (client:Arakoon_client.client) =
  client # user_hook "nsm_host" >>= fun (ic, oc) ->
  Llio.input_int32 ic
  >>= function
  | 0l -> begin
      Lwt_log.debug_f "user hook was found%!" >>= fun () ->
      let client = new single_connection_client (ic, oc) in
      client # query GetVersion () >>= fun (major,minor,patch, commit) ->
      Lwt_log.debug_f "version:(%i,%i,%i,%s)" major minor patch commit >>= fun () ->
      Lwt.return client
    end
  | e ->
    Lwt_log.warning_f "user hook was not found %li%!\n" e
    >>= fun ()->
    Lwt.fail_with "the nsm host user function could not be found"

let make_client tls_config buffer_pool ccfg =
  let tls = Client_helper.get_tls_from_ssl_cfg (Option.map Tls.to_ssl_cfg tls_config) in
  let open Client_helper in
  Lwt_log.debug_f "Nsm_host_client.make_client" >>= fun () ->
  find_master' ?tls ccfg >>= function
  | MasterLookupResult.Found (m, ncfg) ->
     let open Arakoon_client_config in
     let transport = Net_fd.TCP in
     let conn_info = Networking2.make_conn_info ncfg.ips ncfg.port
                                                ~transport
                                                tls_config in
    Networking2.first_connection'
      buffer_pool
      ~conn_info
      ~close_msg:"closing nsm_host"
    >>= fun (fd, conn, closer) ->
    Lwt.catch
      (fun () ->
         Arakoon_remote_client.make_remote_client ccfg.cluster_id conn >>= fun client ->
         wrap_around client)
      (fun exn ->
         closer () >>= fun () ->
         Lwt.fail exn) >>= fun c ->
    Lwt.return (c, closer)
  | r -> Lwt.fail (Client_helper.MasterLookupResult.Error r)


let with_client ccfg tls_config f =
  let open Nsm_model in
  Lwt.catch
    (fun () ->
      make_client tls_config
                  Buffer_pool.default_buffer_pool
                  ccfg
      >>= fun (client, closer) ->
      Lwt.finalize
        (fun () -> f client)
        closer)
    (function
      | Err.Nsm_exn (err, _) as exn ->
        Lwt_log.warning_f ~exn "nsm host client failed with %s" (Err.show err)
        >>= fun () ->
        Lwt.fail exn
      | Client_helper.MasterLookupResult.Error err as exn ->
        Lwt_log.debug_f
          "nsm host client failed with %s"
          (Client_helper.MasterLookupResult.to_string err) >>= fun () ->
        Lwt.fail exn
      | Arakoon_exc.Exception (rc, msg) as exn ->
        Lwt_log.debug_f
          "nsm host client failed with %s: %s"
          (Arakoon_exc.string_of_rc rc) msg >>= fun () ->
        Lwt.fail exn
      | exn ->
        Lwt_log.warning_f ~exn "nsm host client failed with %s\n%!"
                          (Printexc.to_string exn)
        >>= fun () ->
        Lwt.fail exn)
