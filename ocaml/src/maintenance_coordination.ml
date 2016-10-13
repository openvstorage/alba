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

type state =
  | Master
  | Follower

let throttled_log
      cntr
      msg =
  if !cntr = 5
  then
    begin
      cntr := 0;
      Lwt_log.info msg
    end
  else
    begin
      incr cntr;
      Lwt_log.debug msg
    end

class coordinator
        (client : Albamgr_client.client)
        ~name
        ~lease_name ~lease_timeout
        ~registration_prefix
  = object(self)

  val mutable state = Follower
  val mutable last_seen_master_counter = -1

  val mutable registration_counter = 0

  val mutable stop = false

  val mutable modulo = 1
  val mutable remainder = 0
  val mutable on_position_changed : (unit -> unit) list = []

  method trigger_on_position_changed = List.iter ((|>) ()) on_position_changed

  method get_modulo = modulo
  method get_remainder = remainder
  method add_on_position_changed f = on_position_changed <- f :: on_position_changed

  method init =
    Lwt.ignore_result
      begin

        let throttle_log_1 = ref 0 in
        let throttle_log_2 = ref 0 in

        let rec become_master_loop () =
          Lwt.catch
            (fun () ->
             match state with
             | Master ->
                client # try_get_lease lease_name last_seen_master_counter >>= fun () ->
                last_seen_master_counter <- last_seen_master_counter + 1;
                throttled_log
                  throttle_log_1
                  (Printf.sprintf "maintenance_coordination: I (%s) am still the maintenance master (%i)"
                                  name last_seen_master_counter)
             | Follower ->
                client # check_lease lease_name >>= fun counter ->

                if counter = 0 || counter = last_seen_master_counter
                then
                  begin
                    client # try_get_lease lease_name counter >>= fun () ->
                    last_seen_master_counter <- counter + 1;
                    state <- Master;
                    Lwt_log.info_f "maintenance_coordination: I (%s) am now the maintenance master" name
                  end
                else
                  begin
                    last_seen_master_counter <- counter;
                    throttled_log
                      throttle_log_2
                      (Printf.sprintf "maintenance_coordination: Another process is the maintenance master (%i)"
                                      counter)
                  end)
            (fun exn ->
             state <- Follower;
             Lwt_log.info_f ~exn
                            "maintenance_coordination: exception in become_master_loop for %s"
                            name)
          >>= fun () ->
          Lwt_unix.sleep
            (lease_timeout /.
               (if state = Master
                then 2.
                else 1.)) >>= fun () ->
          if stop
          then
            begin
              state <- Follower;
              Lwt.return ()
            end
          else become_master_loop ()
        in

        let rec registration_loop () =
          Lwt.catch
            (fun () ->
             client # register_participant
                    ~prefix:registration_prefix
                    ~name
                    ~counter:registration_counter >>= fun () ->
             registration_counter <- registration_counter + 1;
             Lwt.return ())
            (fun exn ->
             Lwt_log.info_f ~exn "maintenance_coordination: exception in registration loop for %s" name)
          >>= fun () ->
          Lwt_unix.sleep (lease_timeout /. 2.) >>= fun () ->
          if stop
          then Lwt.return_unit
          else registration_loop ()
        in

        let rec check_position_loop previous_participants =
          Lwt.catch
            (fun () ->
             client # get_participants ~prefix:registration_prefix
             >>= fun (cnt, participants) ->

             let modulo' = cnt in
             let remainder' = (List.find_index (fun (name', _) -> name' = name) participants)
                              |> Option.get_some in
             Lwt_log.debug_f "maintenance_coordination: maintenance participant %i of %i" remainder' modulo' >>= fun () ->
             if modulo' <> modulo
                || remainder' <> remainder
             then
               begin
                 Lwt_log.ign_info_f
                   "maintenance_coordination: position changed: from %i of %i to %i of %i"
                   remainder modulo
                   remainder' modulo';
                 modulo <- modulo';
                 remainder <- remainder';
                 self # trigger_on_position_changed
               end;

             (if self # is_master
              then
                (* do cleanup of participants that don't renew their lease *)
                Lwt_list.iter_p
                  (fun (participant, cnt) ->
                   match List.find
                           (fun (name, _) -> name = participant)
                           previous_participants with
                   | None -> Lwt.return_unit
                   | Some (name, cnt') ->
                      if cnt = cnt'
                      then
                        Lwt_log.info_f "maintenance_coordination: removing dead participant %s" name >>= fun () ->
                        client # remove_participant
                                  ~prefix:registration_prefix
                                  ~name:participant
                                  ~counter:cnt
                      else
                        Lwt.return_unit)
                  participants
              else
                Lwt.return_unit) >>= fun () ->
             Lwt.return participants)
            (fun exn ->
             Lwt_log.info_f ~exn "maintenance_coordination: exception in check position loop for %s" name
             >>= fun () ->
             Lwt.return previous_participants)
          >>= fun participants ->
          Lwt_unix.sleep lease_timeout >>= fun () ->
          if stop
          then Lwt.return_unit
          else check_position_loop participants
        in

        stop <- false;
        Lwt.join
          [ become_master_loop ();
            registration_loop ();
            check_position_loop [];
          ] >>= fun () ->
        Lwt.return ()
      end

  method is_master = state = Master

  method stop = stop <- true;
end

let maintenance_lease_timeout = 20.
let maintenance_lease_name = "maintenance"
let maintenance_registration_prefix = "maintenance"

let make_maintenance_coordinator mgr_access =
  let name = Uuidm.v4_gen (Random.State.make_self_init ()) ()
             |> Uuidm.to_string
  in
  new coordinator
      mgr_access
      ~name
