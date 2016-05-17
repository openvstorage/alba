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

class preset_cache (mgr_access : Albamgr_client.client) =
  let get_preset ~preset_name =
    mgr_access # get_preset ~preset_name >>= fun preset_o ->
    let _name, preset, _is_default, _in_use = Option.get_some preset_o in
    Lwt.return preset
  in
  let preset_cache = Hashtbl.create 3 in
  let get_preset_info ~preset_name =
    try Lwt.return (Hashtbl.find preset_cache preset_name)
    with Not_found ->
         get_preset ~preset_name >>= fun preset ->

         if not (Hashtbl.mem preset_cache preset_name)
         then begin
             Lwt.ignore_result begin
                 (* start a thread to refresh the preset periodically *)
                 let rec inner () =
                   Lwt.catch
                     (fun () ->
                      Lwt_extra2.sleep_approx 120. >>= fun () ->
                      get_preset ~preset_name >>= fun preset ->
                      Hashtbl.replace preset_cache preset_name preset;
                      Lwt.return ())
                     (fun exn ->
                      Lwt_log.debug_f
                        ~exn
                        "Error in refresh preset loop %S, ignoring"
                        preset_name) >>= fun () ->
                   inner ()
                 in
                 inner ()
               end;
             Hashtbl.replace preset_cache preset_name preset
           end;
         Lwt.return preset
  in
  object
    method get ~preset_name = get_preset_info ~preset_name
    method refresh ~preset_name =
      (* ensure a refresh thread is already running *)
      get_preset_info ~preset_name >>= fun _ ->
      get_preset ~preset_name >>= fun preset ->
      Hashtbl.replace preset_cache preset_name preset;
      Lwt.return_unit
  end
