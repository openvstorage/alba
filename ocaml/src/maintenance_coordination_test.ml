open Maintenance_coordination
open Lwt.Infix

let test_coordinator_master () =
  Alba_test.test_with_alba_client
    (fun client ->
     let test_name = "test_coordinator_master" in
     let lease_name = test_name in
     let registration_prefix = test_name in
     let lease_timeout = 0.3 in

     (* create a coordinator that we don't initialize
      * to see that it has no influence on the other
      * coordinators which do are initialized
      *)
     let c0 = new coordinator
                 (client#mgr_access)
                 ~name:"c1"
                 ~lease_name
                 ~lease_timeout
                 ~registration_prefix
     in

     let c1 = new coordinator
                  (client#mgr_access)
                  ~name:"c1"
                  ~lease_name
                  ~lease_timeout
                  ~registration_prefix
     in
     c1 # init;
     let c2 = new coordinator
                  (client#mgr_access)
                  ~name:"c2"
                  ~lease_name
                  ~lease_timeout
                  ~registration_prefix
     in
     c2 # init;

     Lwt_unix.sleep (3. *. lease_timeout) >>= fun () ->

     let assert_1_master () =
       assert (not (c0 # is_master));
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

let test_coordinator_participants () =
  Alba_test.test_with_alba_client
    (fun client ->
     let test_name = "test_coordinator_participants" in
     let lease_name = test_name in
     let registration_prefix = test_name in
     let lease_timeout = 0.3 in

     (* create a coordinator that we don't initialize
      * to see that it has no influence on the other
      * coordinators which do are initialized
      *)
     let _ = new coordinator
                 (client#mgr_access)
                 ~name:"c1"
                 ~lease_name
                 ~lease_timeout
                 ~registration_prefix
     in

     let c1 = new coordinator
                  (client#mgr_access)
                  ~name:"c1"
                  ~lease_name
                  ~lease_timeout
                  ~registration_prefix
     in
     c1 # init;
     let c2 = new coordinator
                  (client#mgr_access)
                  ~name:"c2"
                  ~lease_name
                  ~lease_timeout
                  ~registration_prefix
     in
     c2 # init;

     Lwt_unix.sleep (3. *. lease_timeout) >>= fun () ->

     assert (2 = c1 # get_modulo);
     assert (2 = c2 # get_modulo);

     assert (0 = c1 # get_remainder);
     assert (1 = c2 # get_remainder);

     let c3 = new coordinator
                  (client#mgr_access)
                  ~name:"c3"
                  ~lease_name
                  ~lease_timeout
                  ~registration_prefix
     in
     c3 # init;

     Lwt_unix.sleep (2. *. lease_timeout) >>= fun () ->

     assert (3 = c1 # get_modulo);
     assert (3 = c2 # get_modulo);
     assert (3 = c3 # get_modulo);

     assert (0 = c1 # get_remainder);
     assert (1 = c2 # get_remainder);
     assert (2 = c3 # get_remainder);

     c1 # stop;

     Lwt_unix.sleep (4. *. lease_timeout) >>= fun () ->

     assert (2 = c2 # get_modulo);
     assert (2 = c3 # get_modulo);

     assert (0 = c2 # get_remainder);
     assert (1 = c3 # get_remainder);

     Lwt.return ())

open OUnit

let suite = "maintenance_coordination" >:::[
      "test_coordinator_master" >:: test_coordinator_master;
      "test_coordinator_participants" >:: test_coordinator_participants;
    ]
