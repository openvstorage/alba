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

open Prelude
open Lwt
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
        ~exn
        "nsm host operation failed: %i %s" ierr (Err.show ec)
      >>= fun () ->
      Lwt.fail exn
  in
  (object
    method query : type i o.
      ?consistency : Consistency.t ->
      (i, o) query -> i -> o Lwt.t =
      fun ?(consistency = Consistency.Consistent) command req ->
        let buf = Buffer.create 20 in
        let tag = command_to_tag (Wrap_q command) in
        Llio.int32_to buf tag;
        Consistency.to_buffer buf consistency;
        Lwt_log.debug_f "nsm_host_client: %s\n" (tag_to_name tag)
        >>= fun() ->
        write_query_i command buf req;
        Lwt_extra2.llio_output_and_flush oc (Buffer.contents buf) >>= fun () ->
        read_response (read_query_o command)

    method update : type i o. (i, o) update -> i -> o Lwt.t =
      fun command req ->
        let buf = Buffer.create 20 in
        let tag = command_to_tag (Wrap_u command) in
        Llio.int32_to buf tag;
        Lwt_log.debug_f "nsm_host_client: %s\n" (tag_to_name tag)
        >>= fun() ->
        write_update_i command buf req;
        Lwt_extra2.llio_output_and_flush oc (Buffer.contents buf) >>= fun () ->
        read_response (read_update_o command)
  end : basic_client)

class client (client : basic_client) =
  object(self)
    method deliver_message message msg_id =
      client # update
        DeliverMsg
        (message, msg_id)

    method cleanup_for_namespace ~namespace_id =
      client # update
        Nsm_host_protocol.Protocol.CleanupForNamespace
        namespace_id


    method get_version = client # query GetVersion ()

    method statistics (clear:bool) = client # query NSMHStatistics clear
  end

let wrap_around (client:Arakoon_client.client) =
  client # user_hook "nsm_host" >>= fun (ic, oc) ->
  Llio.input_int32 ic
  >>= function
  | 0l -> begin
      Lwt_log.debug_f "user hook was found%!" >>= fun () ->
      let client = new single_connection_client (ic, oc) in
      Lwt.return client
    end
  | e ->
    Lwt_log.warning_f "user hook was not found %li%!\n" e
    >>= fun ()->
    Lwt.fail_with "the nsm host user function could not be found"

let make_client buffer_pool cfg =
  let open Client_helper in
  find_master' ~tls:None cfg >>= function
  | MasterLookupResult.Found (m, ncfg) ->
    let open Arakoon_client_config in
    Networking2.first_connection'
      buffer_pool
      ncfg.ips ncfg.port
      ~close_msg:"closing nsm_host"
    >>= fun (fd, conn, closer) ->
    Lwt.catch
      (fun () ->
         Arakoon_remote_client.make_remote_client cfg.cluster_id conn >>= fun client ->
         wrap_around client)
      (fun exn ->
         closer () >>= fun () ->
         Lwt.fail exn) >>= fun c ->
    Lwt.return (c, closer)
  | r -> Lwt.fail (Client_helper.MasterLookupResult.Error r)


let with_client cfg f =
  let open Nsm_model in
  Lwt.catch
    (fun () ->
       Client_helper.with_master_client'
         ~tls:None
         (Albamgr_protocol.Protocol.Arakoon_config.to_arakoon_client_cfg cfg)
         (fun client ->
            wrap_around client >>= fun wc ->
            f wc))
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
