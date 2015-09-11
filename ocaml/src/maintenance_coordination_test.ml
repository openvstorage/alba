open Maintenance_coordination
open Lwt.Infix

let test_coordinator () =
  Alba_test.test_with_alba_client
    (fun client ->
     let lease_name = "x" in
     let lease_timeout = 0.2 in
     let c1 = new coordinator
                  (client#mgr_access)
                  ~name:"c1"
                  ~lease_name
                  ~lease_timeout
                  ~registration_prefix:""
     in
     c1 # init;
     let c2 = new coordinator
                  (client#mgr_access)
                  ~name:"c2"
                  ~lease_name
                  ~lease_timeout
                  ~registration_prefix:""
     in
     c2 # init;

     Lwt_unix.sleep (2. *. lease_timeout) >>= fun () ->

     let assert_1_master () =
       assert (c1 # is_master || c2 # is_master);
       assert (not (c1 # is_master && c2 # is_master))
     in

     assert_1_master ();
     Lwt_unix.sleep lease_timeout >>= fun () ->
     assert_1_master ();

     let master, slave =
       if c1 # is_master
       then c1, c2
       else c2, c1 in
     master # stop;

     Lwt_unix.sleep (2. *.lease_timeout) >>= fun () ->

     assert (not (master # is_master));
     assert (slave # is_master);

     Lwt.return ())

open OUnit

let suite = "maintenance_coordination" >:::[
      "test_coordinator" >:: test_coordinator;
    ]
