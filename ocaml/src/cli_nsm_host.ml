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

open Cli_common
open Cmdliner
open Lwt.Infix
open Prelude

let nsm_host_statistics (cfg_url:Url.t) tls_config clear nsm_host verbose =
  let t () =
    with_alba_client
      cfg_url
      tls_config
      (fun client ->
       client # nsm_host_access # statistics nsm_host clear >>= fun statistics ->
       Lwt_io.printlf "%s" (Nsm_host_protocol.Protocol.NSMHStatistics.show statistics)
      )
  in
  lwt_cmd_line ~to_json:false ~verbose t

let nsm_host_statistics_cmd =
  Term.(pure nsm_host_statistics
        $ alba_cfg_url
        $ tls_config
        $ clear
        $ nsm_host 0
        $ verbose
  ),
  Term.info
    "nsm-host-statistics"
    ~doc:"namespace hosts' statistics"


let list_device_objects
      cfg_file tls_config osd_id namespace_id first finc max reverse compact
      verbose
  =
  let t () =
    with_alba_client
      cfg_file
      tls_config
      (fun alba_client ->
       alba_client # with_nsm_client'
         ~namespace_id
         (fun nsm_client ->
          nsm_client # list_device_objects
            ~osd_id
            ~first ~finc ~last:None
            ~max ~reverse
          >>= fun ((cnt,xs), more) ->
          Lwt_io.printlf "found %i items:" cnt >>= fun () ->
          let open Nsm_model in
          Lwt_list.iter_s
            (fun mf ->
             if compact
             then Lwt_io.printlf "%s : %S" mf.Manifest.name mf.Manifest.object_id
             else Lwt_io.printlf "%s" (Manifest.show mf)
            ) xs
          >>= fun () ->
          Lwt.return ()
      ))
  in
  lwt_cmd_line ~to_json:false ~verbose t

let list_device_objects_cmd =
  let osd_id default =
    let doc = "osd's short id" in
    Arg.(value
         & opt int32 default
         & info ["osd_id"] ~docv:"OSD_ID" ~doc)
  in
  let namespace_id default =
    let doc = "namespace id" in
    Arg.(value
         & opt int32 default
         & info ["namespace_id"] ~docv:"NAMESPACE_ID" ~doc)
  in
  let make_opt_bool name = Arg.(value & opt bool false & info [name]) in
  let compact = make_opt_bool "compact" in
  let reverse = make_opt_bool "reverse" in
  Term.(pure list_device_objects
        $ alba_cfg_url
        $ tls_config
        $ osd_id 0l
        $ namespace_id 0l
        $ first $ finc
        $ max
        $ reverse
        $ compact
        $ verbose
  ),
  Term.info "dev-list-device-objects"
            ~doc:"objects from the namespace that have fragments on this device"

let cmds =
  [
    nsm_host_statistics_cmd;
    list_device_objects_cmd;
  ]
