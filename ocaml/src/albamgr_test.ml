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

open Prelude
open Albamgr_client
open Lwt
open Albamgr_protocol.Protocol

let host = "127.0.0.1"
let hosts = [ host ]
let ccfg_ref = ref None
let get_ccfg () = Option.get_some (!ccfg_ref)

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
  Lwt_main.run begin
    with_client'
      (get_ccfg ())
      f
  end

let test_add_namespace () =
  test_with_albamgr
    (fun client ->
       client # list_all_namespaces >>= fun (cnt, _) ->
       client # create_namespace ~namespace:"nsb" ~preset_name:None () >>= fun _id ->
       client # list_all_namespaces >>= fun (cnt', _) ->
       assert (cnt' = cnt + 1);
       Lwt.return ())

open OUnit

let suite = "albamgr" >:::[
    "test_add_namespace" >:: test_add_namespace;
  ]
