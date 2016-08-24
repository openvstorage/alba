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
open Albamgr_protocol.Protocol

let host = "127.0.0.1"
let hosts = [ host ]
let ccfg_url_ref = ref None

let get_ccfg_url () = Option.get_some (!ccfg_url_ref)

let _tls_config_ref = ref None
let get_tls_config () = !_tls_config_ref

let assert_throws e msg f =
  Lwt.catch
    (fun () ->
       f () >>= fun () ->
       Lwt.fail (Failure msg))
    (function
      | Error.Albamgr_exn (e', _) when e = e' -> Lwt.return ()
      | Error.Albamgr_exn (e', _) ->
        Printf.printf "Got %s while %s expected\n" (Error.show e') (Error.show e);
        Lwt.return ()
      | exn -> Lwt.fail exn)

let test_with_albamgr f =
  let ccfg_url = get_ccfg_url () in
  let tls_config = !_tls_config_ref in
  let t =
    Alba_arakoon.config_from_url ccfg_url >>= fun ccfg ->
    Albamgr_client.with_client'
       ccfg
       ~tls_config
       f
  in
  Lwt_main.run t



let test_add_namespace () =
  test_with_albamgr
    (fun client ->
       client # list_all_namespaces >>= fun (cnt, _) ->
       client # create_namespace ~namespace:"nsb" ~preset_name:None () >>= fun _id ->
       client # list_all_namespaces >>= fun (cnt', _) ->
       assert (cnt' = cnt + 1);
       Lwt.return ())

let test_unknown_operation () =
  let t =
    begin
      let tls_config =  get_tls_config() in
      let ccfg_url = get_ccfg_url () in
      Alba_arakoon.config_from_url ccfg_url >>= fun ccfg ->
      Albamgr_client._with_client
        ~attempts:1 ccfg
        tls_config
        (fun client ->
         client # do_unknown_operation >>= fun () ->
         let client' = new Albamgr_client.client (client :> Albamgr_client.basic_client) in
         client' # get_alba_id >>= fun _ ->
         client # do_unknown_operation >>= fun () ->
         Lwt.return ())
    end
  in
  Lwt_main.run t

open OUnit

let suite = "albamgr" >:::[
      "test_add_namespace" >:: test_add_namespace;
      "test_unknown_operation" >:: test_unknown_operation;
    ]
