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

open Keystone
open Lwt.Infix
open Cohttp


let username = "admin"
let password = ref ""
let project = "demo"
let url = "http://172.20.54.250:5000/"

let dump resp body =
  let code = resp |> Response.status |> Code.code_of_status in
  Printf.printf "Response code: %d\n" code;
  Printf.printf "Headers: %s\n" (resp |> Response.headers |> Header.to_string);
  Printf.printf "Body of length: %d\n" (String.length body);
  Printf.printf "Body = %s\n" body;
  Lwt.return ()


let test_get_credentials () =
  Lwt_main.run
    begin
      let c = new client ~url ~username ~password:!password ~project () in
      c # get_credentials >>= fun (resp, body) ->
      dump resp (Yojson.Safe.to_string body)
    end

open OUnit

let suite = "keystone" >:::[
      "test_get_credentials" >:: test_get_credentials;
    ]
