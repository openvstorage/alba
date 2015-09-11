open Prelude
open Lwt.Infix

type state =
  | Master
  | Follower

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

  method init =
    Lwt.ignore_result
      begin
        let rec become_master_loop () =
          Lwt.catch
            (fun () ->
             match state with
             | Master ->
                client # try_get_lease lease_name last_seen_master_counter >>= fun () ->
                last_seen_master_counter <- last_seen_master_counter + 1;
                Lwt.return ()
             | Follower ->
                client # check_lease lease_name >>= fun counter ->

                if counter = last_seen_master_counter
                then
                  begin
                    client # try_get_lease lease_name last_seen_master_counter >>= fun () ->
                    last_seen_master_counter <- last_seen_master_counter + 1;
                    state <- Master;
                    Lwt_log.info_f "%s is now the maintenance master" name
                  end
                else
                  begin
                    last_seen_master_counter <- counter;
                    Lwt.return_unit
                  end)
            (fun exn ->
             state <- Follower;
             Lwt_log.info_f ~exn "exception in maintenance coordination loop for %s" name)
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
              stop <- false;
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
             Lwt_log.info_f ~exn "exception in maintenance registration loop for %s" name)
          >>= fun () ->
          Lwt_unix.sleep lease_timeout >>= fun () ->
          registration_loop ()
        in

        let rec check_position_loop previous_participants =
          Lwt.catch
            (fun () ->
             client # get_participants ~prefix:registration_prefix
             >>= fun (cnt, participants) ->

             modulo <- cnt;
             remainder <- (List.find_index (fun (name', _) -> name' = name) participants)
                          |> Option.get_some;

             (if self # is_master
              then
                begin
                  (* TODO look at previous participants and maybe kick some out *)
                  Lwt.return_unit
                end
              else Lwt.return_unit) >>= fun () ->
             Lwt.return participants)
            (fun exn ->
             Lwt_log.info_f ~exn "exception in maintenance check position loop for %s" name
             >>= fun () ->
             Lwt.return previous_participants)
          >>= fun participants ->
          Lwt_unix.sleep lease_timeout >>= fun () ->
          check_position_loop participants
        in

        Lwt.join
          [ become_master_loop ();
            registration_loop ();
            check_position_loop [];
          ]
      end

  method is_master = state = Master

  method stop = stop <- true;
end

let maintenance_lease_timeout = 20.
let maintenance_lease_name = "maintenance"
