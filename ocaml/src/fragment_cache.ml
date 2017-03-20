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

open Lwt_bytes2
open Nsm_model

class virtual cache = object(self)

    (* this method should _never_ throw! *)
    method virtual add :
                     int64 -> string
                     -> Bigstring_slice.t
                     -> (Manifest.t * int64 * string) list Lwt.t

    method add' bid oid blob =
      self # add bid oid blob >>= fun _ -> Lwt.return_unit

    method virtual lookup : timeout:float
                            -> int64 -> string
                            -> (SharedBuffer.t * (Manifest.t * int64 * string) list) option Lwt.t
    method virtual lookup2 : timeout:float
                             -> int64 -> string
                             -> (int * int * Lwt_bytes.t * int) list
                             -> (bool * (Manifest.t * int64 * string) list) Lwt.t

    method virtual drop  : int64 -> global : bool -> unit Lwt.t
    method virtual close : unit -> unit Lwt.t

    method virtual osd_infos : unit ->
                      (Albamgr_protocol.Protocol.alba_id *
                         ((Albamgr_protocol.Protocol.Osd.id * Nsm_model.OsdInfo.t *
                             Capabilities.OsdCapabilities.t))
                           counted_list)
                        counted_list Lwt.t
    method virtual has_local_fragment_cache : bool
end

class no_cache = object(self)
    inherit cache
    method add     bid oid blob   = Lwt.return []
    method lookup  ~timeout bid oid = Lwt.return_none
    method lookup2 ~timeout bid oid slices = Lwt.return (false, [])
    method drop    bid ~global    = Lwt.return_unit
    method close   ()             = Lwt.return_unit
    method osd_infos ()           = Lwt.return (0, [])
    method has_local_fragment_cache = false
end
