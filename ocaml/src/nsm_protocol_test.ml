(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
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
