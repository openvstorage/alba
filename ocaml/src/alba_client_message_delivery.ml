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

open Prelude
open Slice
open Checksum
open Lwt.Infix
open Alba_client_common
module Osd_sec = Osd

let deliver_messages :
type dest msg.
     Albamgr_client.client ->
     Nsm_host_access.nsm_host_access ->
     Osd_access.osd_access ->
     (dest, msg) Albamgr_protocol.Protocol.Msg_log.t ->
     dest ->
     unit Lwt.t
  =
  fun mgr_access nsm_host_access osd_access t dest ->
  let module Osd' = Osd in
  let open Albamgr_protocol.Protocol in
  let rec inner () =
    mgr_access # get_next_msgs t dest >>= fun  ((cnt, msgs), has_more) ->
    Lwt_log.debug_f "deliver_messages: count = %i" cnt >>= fun () ->
    let do_one : type dest msg.
                      (dest, msg) Albamgr_protocol.Protocol.Msg_log.t ->
                      dest ->
                      (int32 * msg) ->
                      unit Lwt.t = fun t dest (msg_id,msg) ->
    begin
      match t with
      | Msg_log.Nsm_host ->
         let nsm_host_id = dest in
         nsm_host_access # get_nsm_host_info ~nsm_host_id
         >>= fun nsm_host_info ->
         if nsm_host_info.Nsm_host.lost
         then Lwt.return_unit
         else
           begin
             Lwt_log.debug_f
               "Delivering msg %li to %s: %s"
               msg_id
               dest
               ([%show : Nsm_host_protocol.Protocol.Message.t] msg) >>= fun () ->
             (nsm_host_access # get ~nsm_host_id:dest) # deliver_message msg msg_id
           end
      | Msg_log.Osd ->
         let osd_id = dest in
         Lwt_log.debug_f
           "Delivering msg %li to %li: %s"
           msg_id
           osd_id
           ([%show : Albamgr_protocol.Protocol.Osd.Message.t] msg) >>= fun () ->
         osd_access # with_osd
           ~osd_id
           (fun client ->
            let module DK = Osd_keys.AlbaInstance in
            client # get_option
                   (osd_access # get_default_osd_priority)
                   (Slice.wrap_string DK.next_msg_id)
            >>= fun next_id_o ->
            let next_id = match next_id_o with
              | None -> 0l
              | Some next_id_s -> deserialize Llio.int32_from (Osd_sec.Blob.to_string_unsafe next_id_s)
            in
            if Int32.(next_id =: msg_id)
            then begin
                let open Albamgr_protocol.Protocol.Osd.Message in
                let asserts, upds =
                  match msg with
                  | AddNamespace (namespace_name, namespace_id) ->
                     let namespace_status_key = DK.namespace_status ~namespace_id in
                     [ Osd'.Assert.none_string namespace_status_key; ],
                     [ Osd'.Update.set_string
                         namespace_status_key
                         Osd'.Osd_namespace_state.(serialize to_buffer Active)
                         Checksum.NoChecksum true;
                       Osd'.Update.set_string
                         (DK.namespace_name ~namespace_id) namespace_name
                         Checksum.NoChecksum true;
                     ]
                in
                let bump_msg_id =
                  Osd'.Update.set_string
                    DK.next_msg_id
                    (serialize Llio.int32_to (Int32.succ next_id))
                    Checksum.NoChecksum
                    true
                in
                let asserts' =
                  Osd'.Assert.value_option
                    (Slice.wrap_string DK.next_msg_id)
                    next_id_o :: asserts
                in
                client # apply_sequence
                       (osd_access # get_default_osd_priority)
                       asserts'
                       (bump_msg_id :: upds)
                >>=
                  let open Osd_sec in
                  (function
                    | Ok    -> Lwt.return ()
                    | Exn x ->
                       Lwt_log.warning ([%show : Error.t] x)
                       >>= fun () ->
                       Error.lwt_fail x
                  )
              end
            else if Int32.(next_id =: succ msg_id)
            then Lwt.return ()
            else
              begin
                let msg =
                  Printf.sprintf
                    "Osd msg_id (%li) too far off"
                    msg_id
                in
                Lwt_log.warning msg >>= fun () ->
                Lwt.fail_with msg
              end
           )
    end >>= fun () ->
    mgr_access #  mark_msg_delivered t dest msg_id
    in
  Lwt_list.iter_s (do_one t dest) msgs >>= fun () ->
  if has_more
  then inner ()
  else Lwt.return ()
  in
  inner ()

let deliver_osd_messages mgr_access nsm_host_access osd_access ~osd_id =
  deliver_messages
    mgr_access nsm_host_access osd_access
    Albamgr_protocol.Protocol.Msg_log.Osd
    osd_id

let deliver_nsm_host_messages mgr_access nsm_host_access osd_access ~nsm_host_id =
  deliver_messages
    mgr_access nsm_host_access osd_access
    Albamgr_protocol.Protocol.Msg_log.Nsm_host
    nsm_host_id

let deliver_all_messages is_master mgr_access nsm_host_access osd_access =
  let deliver_nsm_messages =
    Maintenance_common.maintenance_for_all_x
      "deliver nsm messages"
      (fun () -> mgr_access # list_all_nsm_hosts ())
      (fun (nsm_host_id, _, _) ->
       deliver_nsm_host_messages
         mgr_access nsm_host_access osd_access
         ~nsm_host_id)
      (fun (nsm_host_id, _, _) -> nsm_host_id)
      [%show : (string * Albamgr_protocol.Protocol.Nsm_host.t * int64)]
      is_master
  in

  let deliver_osd_messages =
    Maintenance_common.maintenance_for_all_x
      "deliver osd messages"
      (fun () ->
       let osds =
         Hashtbl.fold
           (fun osd_id _ (cnt, acc) -> cnt+1, osd_id::acc)
           (osd_access # osds_info_cache)
           (0, [])
       in
       Lwt.return osds)
      (fun osd_id ->
       deliver_osd_messages
         mgr_access nsm_host_access osd_access
         ~osd_id)
      Std.id
      Int32.to_string
      is_master
  in
  Lwt.choose [ deliver_nsm_messages;
               deliver_osd_messages;
               osd_access # populate_osds_info_cache; ]

let deliver_messages_to_most_osds
      mgr_access nsm_host_access osd_access
      osd_msg_delivery_threads
      ~osds ~preset =
  let mvar = Lwt_mvar.create_empty () in

  Lwt.ignore_result begin
      let osds_delivered = ref [] in
      let finished = ref false in

      Lwt_list.iter_p
        (fun (osd_id, (_ : Albamgr_protocol.Protocol.Osd.NamespaceLink.state)) ->
         Lwt_extra2.ignore_errors
           (fun () ->
            (if Hashtbl.mem osd_msg_delivery_threads osd_id
             then begin
                 Hashtbl.replace osd_msg_delivery_threads osd_id `Extend;
                 Lwt.return ()
               end else begin
                 let rec inner f =
                   Hashtbl.replace osd_msg_delivery_threads osd_id `Busy;
                   Lwt.finalize
                     (fun () ->
                      deliver_osd_messages
                        mgr_access nsm_host_access osd_access
                        ~osd_id >>=
                        f
                     )
                     (fun () ->
                      match Hashtbl.find osd_msg_delivery_threads osd_id with
                      | `Busy ->
                         Hashtbl.remove osd_msg_delivery_threads osd_id;
                         Lwt.return ()
                      | `Extend ->
                         inner Lwt.return)
                 in
                 inner
                   (fun () ->
                    osds_delivered := osd_id :: !osds_delivered;
                    osd_access # osds_to_osds_info_cache !osds_delivered >>= fun osds_info_cache ->
                    if get_best_policy
                         preset.Albamgr_protocol.Protocol.Preset.policies
                         osds_info_cache = None
                    then Lwt.return ()
                    else begin
                        if not !finished
                        then begin
                            finished := true;
                            Lwt_mvar.put mvar ()
                          end else
                          Lwt.return ()
                      end
                   )
               end)
           ))
        osds
    end;

  Lwt.choose
    [ Lwt_mvar.take mvar;
      Lwt_unix.sleep 2. ]
