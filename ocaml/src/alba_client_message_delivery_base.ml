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

let deliver_messages :
type dest msg.
     Albamgr_client.client ->
     (dest, msg) Albamgr_protocol.Protocol.Msg_log.t ->
     dest ->
     ((Albamgr_protocol.Protocol.Msg_log.id * msg) list -> unit Lwt.t) ->
     unit Lwt.t =
  fun mgr_access
      t dest
      deliver_messages ->
  let rec inner () =
    mgr_access # get_next_msgs t dest >>= fun  ((cnt, msgs), has_more) ->
    if msgs = []
    then Lwt.return ()
    else
      begin
        deliver_messages msgs >>= fun () ->
        mgr_access # mark_msgs_delivered
                   t dest
                   (List.hd_exn msgs |> fst)
                   (List.last_exn msgs |> fst) >>= fun () ->

        if has_more
        then inner ()
        else Lwt.return ()
      end
  in
  inner ()
