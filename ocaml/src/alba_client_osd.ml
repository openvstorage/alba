(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Slice
open Checksum
open Lwt.Infix

let claim_osd mgr_access osd_access ~long_id =
  mgr_access # get_alba_id >>= fun alba_id ->

  mgr_access # check_can_claim ~long_id >>= fun () ->

  mgr_access # get_osd_by_long_id ~long_id >>= fun osd_o ->
  let claim_info, osd_info = Option.get_some osd_o in
  (* TODO check claim_info first (eventhough it's not strictly needed) *)

  let update_osd () =
    osd_access # osd_factory osd_info
    >>= fun ((osd : Osd.osd), closer) ->
    Lwt.finalize
      (fun () ->
       let module IRK = Osd_keys.AlbaInstanceRegistration in
       let open Slice in
       let next_alba_instance' =
         wrap_string IRK.next_alba_instance
       in
       let no_checksum = Checksum.NoChecksum in
       osd # global_kvs # get_option Osd.High next_alba_instance'
       >>= function
       | Some _ ->
          osd # global_kvs # get_exn Osd.High (wrap_string (IRK.instance_log_key 0l))
          >>= fun alba_id' ->
          let u_alba_id' = Lwt_bytes.to_string alba_id' in
          Lwt_log.debug_f
            "osd %s is owned by %s, want to claim for %s"
            long_id u_alba_id' alba_id >>= fun () ->
          if u_alba_id' = alba_id
          then Lwt.return `Continue
          else Lwt.return (`ClaimedBy u_alba_id')
       | None ->
          Lwt_log.debug_f
            "osd %s is not yet owned, let's go claim it"
            long_id >>= fun () ->

          let id_on_osd = 0l in
          let instance_index_key = IRK.instance_index_key ~alba_id in
          let instance_log_key = IRK.instance_log_key id_on_osd in

          let open Osd in

          osd # global_kvs # apply_sequence
              Osd.High
              [ Assert.none next_alba_instance';
                Assert.none_string instance_log_key;
                Assert.none_string instance_index_key; ]
              [ Update.set
                  next_alba_instance'
                  (serialize
                     Llio.int32_to
                     (Int32.succ id_on_osd)
                   |> fun x -> Asd_protocol.Blob.Bytes x)
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
            | Ok _ ->
               Lwt_log.debug_f "successfully claimed osd %s" long_id >>= fun () ->
               Lwt.return `Continue
            | _  ->
               begin
                 osd # global_kvs # get_exn
                     Osd.High
                     (wrap_string (IRK.instance_log_key 0l))
                 >>= fun alba_id'slice ->
                 let alba_id' =  Lwt_bytes.to_string alba_id'slice in
                 Lwt_log.debug_f
                   "got an error while claiming osd %s. it is now owned by %s (wanted to claim for %s)"
                   long_id alba_id' alba_id >>= fun () ->
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
    (fun _ ->
     (* maybe osd wasn't ready yet, try again after a small delay *)
     Lwt_unix.sleep 1.0 >>= fun () ->
     update_osd ())
  >>= function
  | `ClaimedBy alba_id' ->
     let msg = Printf.sprintf
                 "This OSD is already claimed by alba instance %s"
                 alba_id'
     in
     Lwt_log.info msg >>= fun () ->
     Lwt.fail_with msg
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
     Lwt_log.debug_f "Claimed osd %s with id=%Li" long_id osd_id >>= fun () ->
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
                  ([%show: Nsm_model.OsdInfo.t] old_osd_info)
  >>= fun () ->
  Lwt_log.debug_f "updating..." >>= fun () ->
  mgr_access # decommission_osd ~long_id >>= fun () ->
  Hashtbl.remove (osd_access # osds_info_cache) osd_id;
  Lwt.return ()
