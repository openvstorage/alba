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
        ~tcp_keepalive:Tcp_keepalive2.default
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
  Lwt_main.run t

let suite = "Nsm_protocol" >::: [
    "doubles" >:: doubles;
    "test_unknown_operation" >:: test_unknown_operation;
  ]
