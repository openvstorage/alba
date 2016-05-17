(*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
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
