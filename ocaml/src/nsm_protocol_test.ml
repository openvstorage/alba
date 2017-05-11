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

open! Prelude
open Lwt.Infix
open OUnit

let doubles test_ctxt =
 (* this verifies that
       - no command is specified twice
       - no tag is used twice *)
    let h1 = Hashtbl.create 3 in
    let h2 = Hashtbl.create 3 in
    List.iter
      (fun (c, t, _) ->
         assert_bool "not mem h1" (not (Hashtbl.mem h1 c));
         Hashtbl.add h1 c ();
         assert_bool "not mem h2"(not (Hashtbl.mem h2 t));
         Hashtbl.add h2 t ();
         ())
      Nsm_host_protocol.Protocol.command_map

let test_unknown_operation () =
  let t =
    begin
      let open Nsm_host_client in
      let tls_config = Albamgr_test.get_tls_config() in
      Alba_test._fetch_abm_client_cfg () >>= fun ccfg ->
      with_client
        ccfg
        tls_config
        (fun client ->
         client # do_unknown_operation >>= fun () ->
         let client' = new client (client :> basic_client) in
         client' # get_version >>= fun _ ->
         client # do_unknown_operation >>= fun () ->
         Lwt.return ())
    end
  in
  Test_extra.lwt_run t

let suite = "Nsm_protocol" >::: [
    "doubles" >:: doubles;
    "test_unknown_operation" >:: test_unknown_operation;
  ]
