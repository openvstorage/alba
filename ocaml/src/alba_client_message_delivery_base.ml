(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
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
