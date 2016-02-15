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

open Lwt.Infix

include Arakoon_etcd

let store_value peers path value =
  let peers_s = String.concat "," (List.map (fun (h,p) -> Printf.sprintf "%s:%i" h p) peers) in
  let cmd = [| "etcdctl";
               "--peers=" ^ peers_s;
               "get";
               path;
              |]
  in
  Lwt_process.with_process_out
    ("", cmd)
    (fun p ->
     let stdin = p # stdin in
     Lwt_io.write stdin value >>= fun () ->
     Lwt_io.close stdin)
