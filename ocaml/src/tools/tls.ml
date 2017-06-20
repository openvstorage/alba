(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

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

let _maybe_init =
  let init = ref false in
  fun () ->
  begin
    if not !init then
         begin
           Ssl_threads.init ();
           Ssl.init ~thread_safe:true ();
           init := true;
           Lwt_log.ign_info "ssl library initialized"
         end
  end

let to_client_context = function
  | None -> None
  | Some (t:t) ->
     let () = _maybe_init () in
     let ctx = Typed_ssl.create_client_context _PROTOCOL in
     let () = match
         t.creds with
       | None -> ()
       | Some (cert,key) -> Typed_ssl.use_certificate ctx cert key
     in
     Some ctx

let to_server_context = function
  | None -> None
  | Some cfg ->
     let open Asd_config.Config in
     let () = _maybe_init () in
     let ctx = Typed_ssl.create_server_context _PROTOCOL in
     let () = Typed_ssl.use_certificate ctx cfg.cert cfg.key in
     Some ctx
