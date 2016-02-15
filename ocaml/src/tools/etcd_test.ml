(*
Copyright 2016 iNuron NV

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
open Lwt.Infix

let etcd_url =
  try Some (Sys.getenv "ALBA_ETCD")
  with _ -> None

let test_get_set () =
  match etcd_url with
  | None ->
     print_endline "Skipping etcd_test.test_get_set as no etcd is configured"
  | Some url ->
     Lwt_main.run
       begin
         let uri = Uri.of_string url in
         let peers = [ Uri.host uri |> Option.get_some,
                       Uri.port uri |> Option.get_some ] in
         let key = "/test/etcd/get_set" in
         let test value =
           Etcd.store_value peers key value >>= fun () ->
           Etcd.retrieve_value peers key >>= fun value' ->
           assert (value = value');
           Lwt.return ()
         in
         Lwt_list.iter_s
           test
           [ "lala";
             "la la";
             "la\r\n la";
           ]
       end

open OUnit

let suite = "etcd_test" >::: [
    "test_get_set" >:: test_get_set;
  ]
