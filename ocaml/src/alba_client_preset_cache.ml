(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
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
