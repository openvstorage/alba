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

type t = {
    create_key_if_missing : bool;
    project : string;
    (* url, domain, username & password are specified at proxy/maintenance startup *)
  } [@@deriving show, yojson]

let from_buffer buf =
  let container = Llio.string_from buf in
  let ser_version = Llio.int8_from buf in
  assert (ser_version = 1);
  let create_key_if_missing,
      project =
    deserialize
      (Llio.pair_from
         Llio.bool_from
         Llio.string_from) container in
  { create_key_if_missing;
    project; }

let to_buffer buf { create_key_if_missing;
                    project; } =
  let container =
    serialize_with_length
      (Llio.tuple3_to
         Llio.int8_to
         Llio.bool_to
         Llio.string_to)
      (1,
       create_key_if_missing,
       project)
  in
  Llio.string_to buf container

type t' = {
    domain : string;
    username : string;
    password : string;
    url : string;
  }

let conf : t' option ref = ref None
let set_conf c = conf := Some c
let get_conf () = match !conf with
  | None -> failwith "No keystone config available"
  | Some c -> c

let dummy_key = get_random_string 32
let get_key (t : t) (namespace : string) =
  (* TODO fetch key for this namespace from keystone *)
  Lwt.return dummy_key
