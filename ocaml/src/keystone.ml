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
open Lwt.Infix
open Cohttp
open Cohttp_lwt_unix

exception AuthenticationError

let get_json_property (json : Yojson.Safe.json) prop =
  match json with
  | `Assoc props -> List.assoc prop props
  | _ -> None

(* keystone documentation available at
- http://docs.openstack.org/developer/keystone/http-api.html#i-have-a-non-python-client
- http://specs.openstack.org/openstack/keystone-specs/api/v3/identity-api-v3.html
 *)

class client ~url ?(domain="Default") ~username ~password () =
  let make_uri x = Uri.of_string (url ^ x) in
  let to_headers headers_ =
    let headers = Header.init () in
    Header.add_list
      headers
      headers_
  in
  let accept_json_header = "Accept", "application/json" in
  let get url headers =
    Client.get
      ~headers:(to_headers (accept_json_header :: headers))
      (make_uri url)
  in
  let get_json url headers =
    get url headers >>= fun (resp, body) ->
    body |> Cohttp_lwt_body.to_string >>= fun body ->
    let json = Yojson.Safe.from_string body in
    Lwt.return (resp, json)
  in
  let post url body headers =
    Client.post
      ~headers:(to_headers
                  (("Content-length", string_of_int (String.length body))
                   :: accept_json_header
                   :: headers))
      ~body:(Cohttp_lwt_body.of_string body)
      (make_uri url)
  in
  let post_json url body headers =
    post url (Yojson.Safe.to_string body) headers >>= fun (resp, body) ->
    body |> Cohttp_lwt_body.to_string >>= fun body ->
    let json = Yojson.Safe.from_string body in
    Lwt.return (resp, json)
  in
  let get_token ~project =
    (* The deprecated way to authenticate is to pass the username,
     * the user’s domain name (which will default to ‘Default’ if it is not specified),
     * and a password: *)
    let body =
      `Assoc
       [ "auth",
         `Assoc
          [  ("identity",
              `Assoc [
                 ("methods", `List [ `String "password" ]);
                 ("password",
                  `Assoc [ "user",
                           `Assoc [ ("name", `String username);
                                    ("password", `String password);
                                    ("domain", `Assoc [ "name", `String domain ]);
                                  ]
                         ]);
               ]);
             ("scope",
              `Assoc [
                 "project",
                 `Assoc [
                    ("name", `String project);
                    ("domain", `Assoc [ "name", `String domain ]);
                  ]
               ])
          ]
       ]
    in
    Lwt_log.debug_f "Authenticating with keystone @ %s to get a fresh token ..." url >>= fun () ->
    post_json "v3/auth/tokens" body []
    >>= fun (resp, body) ->
    (* TODO get "token" from parsed body *)
    let headers = Response.headers resp in
    Lwt.return (Header.get headers "x-subject-token")
  in
  (* some invalid token to begin with, will be replaced when needed *)
  let tokens = Hashtbl.create 3 in
  let get_cached_token ~project =
    match Hashtbl.find tokens project with
    | None ->
       get_token ~project >>=
         (function
           | None ->
              Lwt.fail AuthenticationError
           | Some token ->
              Hashtbl.replace tokens project token;
              Lwt.return token)
    | Some token ->
       Lwt.return token
  in
  let get_cached_token_header ~project =
    get_cached_token ~project >>= fun token ->
    Lwt.return ("X-Auth-Token", token)
  in
  let authenticated_get ~project url headers =
    let do_request () =
      get_cached_token_header ~project >>= fun token_header ->
      get_json url (token_header :: headers)
    in
    do_request () >>= fun (resp, json) ->

    let unauthorized =
      match get_json_property json "error" with
      | None -> false
      | Some json ->
         match get_json_property json "code" with
         | Some (`Int code) -> code = 401
         | _ -> false
    in

    if unauthorized
    then
      begin
        Hashtbl.remove tokens project;
        do_request ()
      end
    else
      Lwt.return (resp, json)
  in
  object
    method get = get
    method authenticated_get = authenticated_get

    method post = post
    method post_json = post_json

    method get_credentials ~project =
      authenticated_get
        ~project
        "v3/credentials" []
  end
