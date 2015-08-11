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
open Remotes
open Alba_statistics
open Checksum
open Slice
open Lwt.Infix

type osd_state = {
  mutable disqualified : bool;
  mutable write : float list;
  mutable read : float list;
  mutable errors : (float * string) list;
}

let large_value = lazy (String.make (512*1024) 'a')
let osd_buffer_pool = Buffer_pool.osd_buffer_pool

class osd_access mgr_access osd_connection_pool_size =
  let osds_info_cache =
    let open Albamgr_protocol.Protocol in
    (Hashtbl.create 3
     : (Osd.id,
        Osd.t * osd_state) Hashtbl.t) in
  let get_osd_info ~osd_id =
    try Lwt.return (Hashtbl.find osds_info_cache osd_id)
    with Not_found ->
      mgr_access # get_osd_by_osd_id ~osd_id >>= fun osd_o ->
      let osd_info = match osd_o with
        | None -> failwith (Printf.sprintf "could not find osd with id %li" osd_id)
        | Some info -> info
      in
      let osd_state = {
          disqualified = false;
          write = [];
          read = [];
          errors = [];
        } in
      let info' = (osd_info, osd_state) in
      Hashtbl.replace osds_info_cache osd_id info';
      Lwt.return info'
  in

  let osds_pool =
    Pool.Osd.make
      ~size:osd_connection_pool_size
      (fun osd_id ->
         get_osd_info ~osd_id >>= fun (osd_info, _) ->
         let open Albamgr_protocol.Protocol in
         Lwt.return osd_info.Osd.kind)
      osd_buffer_pool
  in

  let rec with_osd_from_pool
          : type a.
                 osd_id:Albamgr_protocol.Protocol.Osd.id ->
                        (Osd.osd -> a Lwt.t) -> a Lwt.t
  = fun ~osd_id f ->
  Lwt.catch
    (fun () ->
     Pool.Osd.use_osd
       osds_pool
       ~osd_id
       f)
    (fun exn ->
     get_osd_info ~osd_id >>= fun (_, state) ->

     let add_to_errors =
       let open Asd_protocol.Protocol.Error in
       match exn with
       | End_of_file
       | Exn Assert_failed _ -> false
       | _ -> true
     in
     if add_to_errors
     then state.errors <- (Unix.gettimeofday (),
                           Printexc.to_string exn) :: state.errors;

     let should_invalidate_pool =
       match exn with
       | Asd_client.BadLongId _
       (* TODO need a similar detection + exception for the kinetics *)
       | _ when Networking2.is_connection_failure_exn exn ->
          (* don't want this osd to be chosen for now *)
          true
       | exn ->
          false
     in
     if should_invalidate_pool
     then Pool.Osd.invalidate osds_pool ~osd_id;

     (if add_to_errors || should_invalidate_pool
      then disqualify ~osd_id
      else Lwt.return ()) >>= fun () ->

     Lwt_log.debug_f ~exn "Exception in with_osd_from_pool osd_id=%li" osd_id >>= fun () ->
     Lwt.fail exn)
  and disqualify ~osd_id =
    let rec inner delay =
      Lwt.catch
        (fun () ->
         mgr_access # get_osd_by_osd_id ~osd_id >>= function
         | None -> Lwt.return `OkAgain
         | Some osd_info ->
            with_osd_from_pool ~osd_id
                               (fun osd ->
                                Statistics.with_timing_lwt
                                  (fun () ->
                                   osd # apply_sequence
                                       []
                                       [ Osd.Update.set_string
                                           Osd_keys.test_key
                                           (Lazy.force large_value)
                                           Checksum.NoChecksum
                                           false ])
                                >>= function
                                | (t, Osd.Ok) ->
                                   if t < 1.
                                   then Lwt.return `OkAgain
                                   else Lwt.return (`Continue Lwt_unix.Timeout)
                                | (_, Osd.Exn err) ->
                                   Lwt.return (`Continue (Osd.Error.Exn err))))
        (fun exn -> Lwt.return (`Continue exn))
      >>= function
      | `Continue exn ->
         Lwt_log.debug_f
           ~exn
           "Could not yet requalify osd %li, trying again in %f seconds"
           osd_id delay >>= fun () ->
         Lwt_extra2.sleep_approx delay >>= fun () ->
         inner (min (delay *. 1.5) 60.)
      | `OkAgain ->
         Lwt.return ()
    in
    get_osd_info ~osd_id >>= fun (_, state) ->
    if state.disqualified
    then Lwt.return ()
    else begin
        state.disqualified <- true;
        Lwt_log.debug_f "Disqualifying osd %li" osd_id >>= fun () ->
        (* start loop to get it requalified... *)
        Lwt.async
          (fun () ->
           inner 1. >>= fun () ->
           Lwt_log.debug_f "Requalified osd %li" osd_id >>= fun () ->
           state.disqualified <- false;
           state.write <- Unix.gettimeofday() :: state.write;
           Lwt.return ());
        Lwt.return ()
      end
  in
  let osd_long_id_claim_info = ref StringMap.empty in
  object
    method with_osd :
             'a. osd_id : Albamgr_protocol.Protocol.Osd.id ->
                          (Osd.osd -> 'a Lwt.t) -> 'a Lwt.t =
      fun ~osd_id f ->
      with_osd_from_pool ~osd_id f

    method get_osd_info = get_osd_info

    method osds_info_cache = osds_info_cache

    method osds_to_osds_info_cache osds =
      let res = Hashtbl.create 0 in
      Lwt_list.iter_p
        (fun osd_id ->
         let open Albamgr_protocol.Protocol in
         get_osd_info ~osd_id >>= fun (osd_info, osd_state) ->
         let osd_ok =
           not osd_state.disqualified &&
             not osd_info.Osd.decommissioned &&
               (match osd_state.write, osd_state.errors with
               | [], [] -> true
                | [], _ -> false
                | _, [] -> true
                | write_time::_, (error_time, _)::_ -> write_time > error_time)
         in
         if osd_ok
         then Hashtbl.add
                res
                osd_id
                osd_info;
         Lwt.return ())
        osds >>= fun () ->
      Lwt.return res

    method seen ?(check_claimed_delay=60.) =
      let open Discovery in
      function
      | Bad s ->
         Lwt_log.info_f "Got 'bad' broadcast message: %s" s
      | Good (json, { id; extras; ips; port; }) ->

         let open Albamgr_protocol.Protocol in

         let node_id, kind, total, used =
           match extras with
           | None ->
              id,
              Osd.Kinetic (ips, port, id),
              Int64.shift_left 1L 42 (* 4TB*),
              Int64.shift_left 1L 41 (* TODO: half full? *)
           | Some { node_id; version; total; used } ->
              node_id,
              Osd.Asd (ips, port, id),
              total,
              used
         in

         if StringMap.mem id !osd_long_id_claim_info
         then begin
             let open Osd.ClaimInfo in
             match StringMap.find id !osd_long_id_claim_info with
             | AnotherAlba _
             | Available ->
                Lwt.return ()
             | ThisAlba osd_id ->

                get_osd_info ~osd_id >>= fun (osd_info, osd_state') ->
                let read',
                    write',
                    errors' = osd_state'.read,
                              osd_state'.write,
                              osd_state'.errors in
                osd_state'.read <- [];
                osd_state'.write <- [];
                osd_state'.errors <- [];
                let ips', port' = Osd.get_ips_port osd_info.Osd.kind in

                Hashtbl.replace
                  osds_info_cache
                  osd_id
                  (Osd.({ osd_info with
                          kind;
                          total; used;
                        }),
                   osd_state');

                (* TODO compare ips as a set? (such that ordering doesn't matter) *)
                (if ips' <> ips || port' <> port
                 then begin
                     Lwt_log.debug_f "Asd now has new ips/port -> invalidating connection pool" >>= fun () ->
                     Pool.Osd.invalidate osds_pool ~osd_id;
                     Lwt.return ()
                   end else
                   Lwt.return ()) >>= fun () ->

                mgr_access # update_osd ~long_id:id
                           (Osd.Update.make
                              ~ips':ips ~port':port
                              ~total':total ~used':used
                              ~seen':[Unix.gettimeofday ()]
                              ~read' ~write' ~errors'
                              ~other':json ()
                           )
           end else begin

             let osd_info =
               Osd.({
                       kind;
                       decommissioned = false;
                       node_id;
                       other = json;
                       total; used;
                       seen = [ Unix.gettimeofday (); ];
                       read = [];
                       write = [];
                       errors = [];
                     }) in

             Lwt.catch
               (fun () -> mgr_access # add_osd osd_info)
               (function
                 | Error.Albamgr_exn (Error.Osd_already_exists, _) -> Lwt.return ()
                 | exn -> Lwt.fail exn)
             >>= fun () ->

             let get_claim_info () =
               mgr_access # get_osd_by_long_id ~long_id:id >>= fun osd_o ->
               let claim_info, osd_info = Option.get_some osd_o in
               Lwt.return claim_info
             in

             get_claim_info () >>= fun claim_info ->

             let open Osd.ClaimInfo in
             match claim_info with
             | ThisAlba _
             | AnotherAlba _ ->
                osd_long_id_claim_info := StringMap.add id claim_info !osd_long_id_claim_info;
                Lwt.return ()
             | Available ->
                if not (StringMap.mem id !osd_long_id_claim_info)
                then begin
                    osd_long_id_claim_info := StringMap.add id claim_info !osd_long_id_claim_info;
                    Lwt.ignore_result begin
                        (* start a thread to track it's claimed status
                           and update in the albamgr if needed *)
                        let rec inner () =
                          Lwt.catch
                            (fun () ->
                             Pool.Osd.factory osd_buffer_pool kind >>= fun (osd, closer) ->
                             Lwt.finalize
                               (fun () ->
                                osd # get_option
                                    (Slice.wrap_string
                                       (Osd_keys.AlbaInstanceRegistration.instance_log_key 0l)))
                               closer >>= function
                             | None -> Lwt.return `Continue
                             | Some alba_id' ->
                                let alba_id' = Slice.get_string_unsafe alba_id' in
                                mgr_access # get_alba_id >>= fun alba_id ->

                                Lwt.catch
                                  (fun () ->
                                   if alba_id' = alba_id
                                   then begin
                                       mgr_access # mark_osd_claimed ~long_id:id >>= fun _ ->
                                       Lwt.return ()
                                     end else
                                     mgr_access # mark_osd_claimed_by_other
                                                ~long_id:id
                                                ~alba_id:alba_id')
                                  (function
                                    | Error.Albamgr_exn (Error.Osd_already_claimed, _) ->
                                       Lwt.return ()
                                    | exn -> Lwt.fail exn) >>= fun () ->

                                (* get osd_claim_info and store it... *)
                                get_claim_info () >>= fun claim_info ->
                                osd_long_id_claim_info :=
                                  StringMap.add id claim_info !osd_long_id_claim_info;

                                Lwt.return `Stop)
                            (fun exn ->
                             Lwt_log.debug_f
                               ~exn
                               "Error during check osd claimed thread (for %s)"
                               id >>= fun () ->
                             Lwt.return `StopAndRemove) >>= function
                          | `Continue ->
                             Lwt_extra2.sleep_approx check_claimed_delay >>= fun () ->
                             inner ()
                          | `Stop ->
                             Lwt.return ()
                          | `StopAndRemove ->
                             (* forget we ever saw this osd (if needed it will be picked up again) *)
                             osd_long_id_claim_info := StringMap.remove id !osd_long_id_claim_info;
                             Lwt.return ()
                        in
                        inner ()
                      end;
                    Lwt.return ()
                  end else
                  Lwt.return ()
           end

  end
