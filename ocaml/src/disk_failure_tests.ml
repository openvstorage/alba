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
open Nsm_model
open Lwt
open Albamgr_protocol.Protocol
open OUnit

let _easiest_upload () =
  let cfg_file = "./cfg/test.ini"
  and namespace = "demo"
  and input_file = "./ocaml/alba.native"
  and object_name = Printf.sprintf "easy_test_%i" 1
  and allow_overwrite = false in
  let cfg = Arakoon_config.from_config_file cfg_file in

  let shoot (sid,sinfo) =
    match sinfo.Osd.kind with
      | Osd.Asd (ips,port,long_id) ->
         begin
           let ip = List.hd_exn ips in
           Lwt_io.printlf "going to kill: %li (%s,%i) ASD"  sid ip port
           >>=fun()->
           let cmd = Printf.sprintf "pkill -f 'alba.native.*%i.*'" port in
           let rc = Sys.command cmd in
           Lwt_io.printlf "rc=%i" rc
         end
      | Osd.Kinetic(ips,port, long_id) ->
         begin
           let ip = List.hd_exn ips in
           Lwt_io.printlf "going to kill: %li (%s,%i) Kinetic" sid ip port
           >>= fun () ->
           let cmd = Printf.sprintf "pkill -f 'java.*-port %i.*'" port in
           let rc = Sys.command cmd in
           Lwt_io.printlf "rc=%i" rc
         end
  in

  Alba_client.with_client
    (ref cfg)
    (fun alba_client ->

     alba_client # mgr_access # list_all_claimed_osds >>= fun (n, osds) ->
     Lwt_io.printlf "n=%i" n >>= fun()->
     let soon_dead = List.hd_exn osds
     in
     shoot soon_dead >>=fun()->
     Lwt_io.printlf "uploading"
     >>=fun()->

     alba_client # upload_object_from_file
       ~namespace
       ~object_name ~input_file
       ~checksum_o:None
       ~allow_overwrite:(if allow_overwrite
                         then Unconditionally
                         else NoPrevious)
     >>= fun (manifest, _) ->
     Lwt_io.printlf "manifest=%s" ([%show : Nsm_model.Manifest.t] manifest)
     >>= fun()->
     let open Manifest in
     let chunks = manifest.fragment_locations in
     let osd_id_o, version_id = List.hd_exn (List.hd_exn chunks) in
     Lwt_io.printlf "osd_id:%li" (Option.get_some osd_id_o))


let easiest_upload ctx =
  Lwt_main.run (_easiest_upload())

let () =
  let suite =
    "disk_failures" >:::
       ["easiest_upload" >:: easiest_upload;]

  in
  let produce_xml = Array.length Sys.argv >= 2
                    && Sys.argv.(1) = "--xml=true"
  in
  let _results =
    if produce_xml
    then OUnit_XML.run_suite_producing_xml suite "testresults.xml"
    else OUnit.run_test_tt_main suite
  in
  ()
