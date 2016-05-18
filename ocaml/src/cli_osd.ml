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

open Cmdliner
open Lwt.Infix
open Prelude
open Slice
open Cli_common

let osd_id =
  let doc = "$(docv) of the osd to connect with" in
  Arg.(required
       & opt (some int32) None
       & info ["osd-id"] ~docv:"OSD_ID" ~doc)

let unescape =
  Arg.(value
       & flag
       & info ["unescape"] ~docv:"unescape keys")

let osd_multi_get osd_id cfg_file tls_config keys unescape verbose =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
         client # with_osd ~osd_id
           (fun osd ->
              Lwt_list.map_s
                (fun key ->
                   (osd # global_kvs) # get_option
                     Osd.High
                     (Slice.wrap_string (if unescape
                      then Scanf.unescaped key
                      else key)))
                keys >>= fun values_s ->
              let values = List.map
                             (fun v -> Option.map Lwt_bytes.to_string v)
                             values_s in
              Lwt_io.printlf "%s" ([%show : string option list] values)))
  in
  lwt_cmd_line ~to_json:false ~verbose t

let osd_multi_get_cmd =
  let keys = Arg.(non_empty &
                 pos_all string [] &
                 info [] ~docv:"KEYS" ~doc:"$(docv)") in
  Term.(pure osd_multi_get $ osd_id
        $ alba_cfg_url
        $ tls_config $ keys
        $ unescape
        $ verbose
  ),
  Term.info "osd-multi-get" ~doc:"multi get on an OSD"

let osd_range osd_id cfg_file tls_config verbose =
  let t () =
    with_alba_client
      cfg_file tls_config
      (fun client ->
       let first = Slice.wrap_string "" in
         client # with_osd ~osd_id
           (fun osd ->
            osd # global_kvs # range
                Osd.High
                ~first ~finc:true
                ~last:None
                ~reverse:false ~max:(-1) >>= fun ((cnt, keys_s), has_more) ->
              let keys = List.map Slice.get_string_unsafe keys_s in
              Lwt_io.printlf "%s"
                ([%show : string list] keys)))
  in
  lwt_cmd_line ~to_json:false ~verbose t

let osd_range_cmd =
  Term.(pure osd_range
        $ osd_id
        $ alba_cfg_url
        $ tls_config
        $ verbose),
  Term.info "osd-range" ~doc:"range query on an OSD"

let alba_claim_osd alba_cfg_file tls_config long_id to_json verbose =
  let t () =
    with_alba_client
      alba_cfg_file
      tls_config
      (fun alba_client ->
         alba_client # claim_osd ~long_id) >>= fun osd_id ->
    Lwt_log.debug_f "Claimed %S with id=%li" long_id osd_id
  in
  lwt_cmd_line_unit ~to_json ~verbose t

let alba_claim_osd_cmd =
  Term.(pure alba_claim_osd
        $ alba_cfg_url
        $ tls_config
        $ Arg.(required
               & opt (some string) None
               & info ["long-id"] ~docv:"LONG_ID")
        $ to_json $ verbose),
  Term.info
    "claim-osd"
    ~doc:"claim the osd by this alba instance"

let alba_decommission_osd alba_cfg_file tls_config long_id to_json verbose =
  let t () =
    with_alba_client
      alba_cfg_file tls_config
      (fun alba_client -> alba_client # decommission_osd ~long_id)
  in
  lwt_cmd_line_unit ~to_json ~verbose t

let alba_decommission_osd_cmd =
  Term.(pure alba_decommission_osd
        $ alba_cfg_url
        $ tls_config
        $ Arg.(required
               & opt (some string) None
               & info ["long-id"] ~docv:"LONG_ID"
          )
        $ to_json $ verbose
  ),
  Term.info
    "decommission-osd"
    ~doc:"tell this alba instance to no longer use the osd"

let alba_purge_osd alba_cfg_file tls_config long_id to_json verbose =
  let t () =
    with_alba_client
      alba_cfg_file tls_config
      (fun alba_client -> alba_client # mgr_access # purge_osd ~long_id)
  in
  lwt_cmd_line_unit ~to_json ~verbose t

let alba_purge_osd_cmd =
  Term.(pure alba_purge_osd
        $ alba_cfg_url
        $ tls_config
        $ Arg.(required
               & opt (some string) None
               & info ["long-id"] ~docv:"LONG_ID"
          )
        $ to_json $ verbose
  ),
  Term.info
    "purge-osd"
    ~doc:"tell this alba instance this osd is no longer available"

let cmds = [
    osd_multi_get_cmd;
    osd_range_cmd;
    alba_claim_osd_cmd;
    alba_decommission_osd_cmd;
    alba_purge_osd_cmd;
  ]
