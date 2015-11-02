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
open Remotes
open Osd_access
open Lwt.Infix

let claim_osd mgr_access osd_access ~long_id =
  mgr_access # get_alba_id >>= fun alba_id ->

  mgr_access # check_can_claim ~long_id >>= fun () ->

  mgr_access # get_osd_by_long_id ~long_id >>= fun osd_o ->
  let claim_info, osd_info = Option.get_some osd_o in
  (* TODO check claim_info first (eventhough it's not strictly needed) *)

  let update_osd () =
    Pool.Osd.factory osd_buffer_pool osd_info.Albamgr_protocol.Protocol.Osd.kind
    >>= fun (osd, closer) ->
    Lwt.finalize
      (fun () ->
       let module IRK = Osd_keys.AlbaInstanceRegistration in
       let open Slice in
       let next_alba_instance' =
         wrap_string IRK.next_alba_instance
       in
       let no_checksum = Checksum.NoChecksum in
       osd # get_option Osd.High next_alba_instance'
       >>= function
       | Some _ ->
          osd # get_exn Osd.High (wrap_string (IRK.instance_log_key 0l))
          >>= fun alba_id' ->
          let u_alba_id' = get_string_unsafe alba_id' in
          if u_alba_id' = alba_id
          then Lwt.return `Continue
          else Lwt.return (`ClaimedBy u_alba_id')
       | None ->
          (* the osd is not yet owned, go and tag it *)

          let id_on_osd = 0l in
          let instance_index_key = IRK.instance_index_key ~alba_id in
          let instance_log_key = IRK.instance_log_key id_on_osd in

          let open Osd in

          osd # apply_sequence
              Osd.High
              [ Assert.none next_alba_instance';
                Assert.none_string instance_log_key;
                Assert.none_string instance_index_key; ]
              [ Update.set
                  next_alba_instance'
                  (wrap_string (serialize Llio.int32_to (Int32.succ id_on_osd)))
                  no_checksum true;
                Update.set_string
                  instance_log_key
                  alba_id
                  no_checksum true;
                Update.set_string
                  instance_index_key
                  (serialize Llio.int32_to id_on_osd)
                  no_checksum true; ]
          >>=
            function
            | Ok -> Lwt.return `Continue
            | _  ->
               begin
                 osd # get_exn
                     Osd.High
                     (wrap_string (IRK.instance_log_key 0l))
                 >>= fun alba_id'slice ->
                 let alba_id' = get_string_unsafe alba_id'slice in
                 let r =
                   if alba_id' = alba_id
                   then `Continue
                   else `ClaimedBy alba_id'
                 in
                 Lwt.return r
               end
      )
      (fun () -> closer ())
  in

  Lwt.catch
    update_osd
    (function
      | exn when Networking2.is_connection_failure_exn exn ->
         Lwt_unix.sleep 1.0 >>= fun () ->
         update_osd ()
      | exn -> Lwt.fail exn)
  >>= function
  | `ClaimedBy alba_id' ->
     Lwt_io.printlf "This OSD is already claimed by alba instance %s" alba_id' >>= fun () ->
     Lwt.fail_with "Failed to add OSD because it's already claimed by another alba instance"
  | `Continue ->
     let open Albamgr_protocol.Protocol in
     Lwt.catch
       (fun () -> mgr_access # mark_osd_claimed ~long_id)
       (function
         | Error.Albamgr_exn (Error.Osd_already_claimed, _) ->
            Lwt_log.debug_f
              "Osd is already claimed on the albamgr, this could be a race with the maintenance process picking this up" >>= fun () ->
            mgr_access # get_osd_by_long_id ~long_id >>= fun r ->
            let claim_info, osd_info = Option.get_some r in
            let open Osd.ClaimInfo in
            (match claim_info with
             | ThisAlba id -> Lwt.return id
             | AnotherAlba _
             | Available ->
                Lwt_log.info_f
                  "Osd is already claimed, but strangely enough not by this instance ... consider this a bug" >>= fun () ->
                Lwt.fail_with "Osd is already claimed, but strangely enough not by this instance ... consider this a bug")
         | exn -> Lwt.fail exn)
     >>= fun osd_id ->
     Lwt_log.debug_f "Claimed osd %s with id=%li" long_id osd_id >>= fun () ->
     (* the statement below is for the side effect of filling the cache *)
     osd_access # get_osd_info ~osd_id >>= fun _ ->
     Lwt.return osd_id

let decommission_osd mgr_access osd_access ~long_id =
  mgr_access # get_osd_by_long_id ~long_id >>= fun r ->
  let claim_info, old_osd_info = Option.get_some r in
  let open Albamgr_protocol.Protocol.Osd in

  (match claim_info with
   | ClaimInfo.ThisAlba id ->
      Lwt.return id
   | _ ->
      Lwt.fail_with "can not decommission an osd which isn't yet claimed")
  >>= fun osd_id ->

  Lwt_log.debug_f "old_osd_info:\n%s"
                  ([%show: Albamgr_protocol.Protocol.Osd.t] old_osd_info)
  >>= fun () ->
  Lwt_log.debug_f "updating..." >>= fun () ->
  mgr_access # decommission_osd ~long_id >>= fun () ->
  Hashtbl.remove (osd_access # osds_info_cache) osd_id;
  Lwt.return ()
