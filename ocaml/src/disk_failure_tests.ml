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

open! Prelude
open Nsm_model
open Lwt.Infix
open OUnit
open Cmdliner

let cfg_url = ref (Url.File "./cfg/test.ini")
let tls_config = ref None

let _easiest_upload () =
  let tls_config = !tls_config
  and namespace = "demo"
  and input_file = "./ocaml/alba.native"
  and object_name = Printf.sprintf "easy_test_%i" 1
  and allow_overwrite = false in
  let shoot (sid,sinfo) =
    let open Nsm_model in
    match sinfo.OsdInfo.kind with
      | OsdInfo.Asd (conn_info,long_id) ->
         begin
           let ips,port,_,_ = conn_info in
           let ip = List.hd_exn ips in
           Lwt_io.printlf "going to kill ASD: %Li (%s,%i)"  sid ip port
           >>=fun()->
           let cmd = Printf.sprintf "pkill -f 'alba.native.*%02Li.*'" sid in
           Printf.printf "cmd=%S\n%!" cmd;
           let rc = Sys.command cmd in
           Lwt_io.printlf "rc=%i" rc
         end
      | OsdInfo.Kinetic(conn_info, long_id) ->
         begin
           let ips,port,_,_ = conn_info in
           let ip = List.hd_exn ips in
           Lwt_io.printlf "going to kill Kinetic: %Li (%s,%i)" sid ip port
           >>= fun () ->
           let cmd = Printf.sprintf "pkill -f 'java.*-port %i.*'" port in
           let rc = Sys.command cmd in
           Lwt_io.printlf "rc=%i" rc
         end
      | OsdInfo.Alba _
      | OsdInfo.Alba2 _
      | OsdInfo.AlbaProxy _ -> assert false
  in
  Alba_arakoon.config_from_url !cfg_url >>= fun cfg ->
  Alba_client2.with_client
    (ref cfg)
    ~tls_config
    ~populate_osds_info_cache:true
    ~upload_slack:1000.
    (fun alba_client ->
     alba_client # create_namespace ~namespace:"disk_failure_test" ~preset_name:None () >>= fun namespace_id ->
     Alba_test._wait_for_osds ~cnt:6 alba_client namespace_id >>= fun () ->
     alba_client # mgr_access # list_all_claimed_osds >>= fun (n, osds) ->
     Lwt_io.printlf "there are n=%i claimed osds" n >>= fun()->
     let soon_dead = List.hd_exn osds
     in
     shoot soon_dead >>=fun()->
     Lwt_io.printlf "uploading"
     >>=fun()->

     alba_client # upload_object_from_file
       ~epilogue_delay:None
       ~namespace
       ~object_name ~input_file
       ~checksum_o:None
       ~allow_overwrite:(if allow_overwrite
                         then Unconditionally
                         else NoPrevious)
     >>= fun (manifest,_, _,_) ->
     Lwt_io.printlf "manifest=%s" ([%show : Nsm_model.Manifest.t] manifest)
     >>= fun()->
     let osd_id_o, version_id = Manifest.get_location manifest 0 0 in
     Lwt_io.printlf "osd_id:%Li" (Option.get_some osd_id_o))


let easiest_upload ctx =
  let () =
    Lwt_log.default :=
      Lwt_log.channel
        ~channel:Lwt_io.stdout
        ~close_mode:`Keep
        ~template:"$(date).$(milliseconds) $(message)"
        ()
  in
  let () = Lwt_log_core.append_rule "*" Lwt_log_core.Info
  in
  Lwt_engine.set (new Lwt_rsocket.rselect);


  let dump_stats =
    let t0 = Unix.gettimeofday() in
    fun () ->
    Alba_wrappers.Sys2.lwt_get_maxrss () >>= fun maxrss ->
        let stat = Gc.quick_stat () in
        let factor = (float (Sys.word_size / 8)) /. 1024.0 in
        let mem_allocated = (stat.Gc.minor_words +. stat.Gc.major_words -. stat.Gc.promoted_words)
                            *. factor in
        let msg = Printf.sprintf
                    "maxrss:%i KB, allocated:%f, minor_collections:%i, major_collections:%i, compactions:%i, heap_words:%i"
                    maxrss mem_allocated stat.Gc.minor_collections
                    stat.Gc.major_collections stat.Gc.compactions
                    stat.Gc.heap_words
        in
        Lwt_log.info msg >>= fun () ->
        Lwt_io.printlf "%f: %s%!" (Unix.gettimeofday() -. t0) msg
  in
  let rec report () =
    dump_stats () >>= fun () ->
    Lwt_unix.sleep 1.0 >>= fun () ->
    report ()
  in
  let t () =
    Lwt.finalize
      (fun () ->
        dump_stats () >>= fun () ->
          _easiest_upload())
      (fun () ->
        dump_stats ()

      )
  in
  let combined = Lwt.pick [t (); report ()] in
  Test_extra.lwt_run combined

let () =
  let suite =
    "disk_failures" >:::
       ["easiest_upload" >:: easiest_upload;]

  in
  let run_unit_tests alba_cfg_url tls_config' produce_xml =
    let () = cfg_url := alba_cfg_url in
    let () = tls_config := tls_config' in
    let _results =
      if produce_xml
      then OUnit_XML.run_suite_producing_xml suite "testresults.xml"
      else OUnit.run_test_tt_main suite
    in
    ()
  in
  let run_unit_tests_cmd =
    Term.(pure run_unit_tests
          $ Cli_common.alba_cfg_url
          $ Cli_common.tls_config
          $ Cli_common.produce_xml false
    ),
    Term.info "unit-tests" ~doc:"run unit tests"
  in
  let cmds = [run_unit_tests_cmd] in
  let default_cmd = run_unit_tests_cmd in
  match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1
  | _ -> exit 0
