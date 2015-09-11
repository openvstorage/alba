open Lwt.Infix

type state =
  | Master
  | Follower

class coordinator
        (client : Albamgr_client.client)
        ~name
        ~lease_name ~lease_timeout = object

  val mutable state = Follower
  val mutable last_seen_counter = -1
  val mutable stop = false

  method init =
    Lwt.ignore_result
      begin
        let rec inner () =
          Lwt.catch
            (fun () ->
             match state with
             | Master ->
                client # try_get_lease lease_name last_seen_counter >>= fun () ->
                last_seen_counter <- last_seen_counter + 1;
                Lwt.return ()
             | Follower ->
                client # check_lease lease_name >>= fun counter ->

                if counter = last_seen_counter
                then
                  begin
                    client # try_get_lease lease_name last_seen_counter >>= fun () ->
                    last_seen_counter <- last_seen_counter + 1;
                    state <- Master;
                    Lwt_log.info_f "%s is now the maintenance master" name
                  end
                else
                  begin
                    last_seen_counter <- counter;
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
          else inner ()
        in
        inner ()
      end

  method is_master = state = Master

  method stop = stop <- true;
end

let maintenance_lease_timeout = 20.
let maintenance_lease_name = "maintenance"
