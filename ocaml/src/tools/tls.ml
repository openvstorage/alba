(*
Copyright 2015 Open vStorage NV

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

let _PROTOCOL = Ssl.TLSv1

type t = {
    ca_cert : string;
    creds : (string * string) option [@default None];
    (* TODO: fit protocol in here *)
  }[@@deriving yojson, show]


let make (ca_cert,cert,key) = {
    ca_cert;
    creds = Some (cert,key);
  }

let of_ssl_cfg (x:Arakoon_client_config.ssl_cfg)=
  let open Arakoon_client_config in
  let () = assert (x.protocol = _PROTOCOL) in
  {
    ca_cert = x.ca_cert;
    creds = x.creds;
  }

let to_ssl_cfg (t:t) =
  let open Arakoon_client_config in
  { ca_cert = t.ca_cert;
    creds = t.creds;
    protocol = _PROTOCOL
  }

let to_context =
  let init = ref false in
  function
  | None -> None
  | Some (t:t) ->
     let () =
       if not !init then
         begin
           Ssl_threads.init ();
           Ssl.init ~thread_safe:true ();
           init := true;
           Lwt_log.ign_info "ssl library initialized"
         end
     in
     let ctx = Typed_ssl.create_client_context _PROTOCOL in
     let () = match
         t.creds with
       | None -> ()
       | Some (cert,key) -> Typed_ssl.use_certificate ctx cert key
     in
     Some ctx
