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

let long_id =
  Arg.(required
       & opt (some string) None
       & info ["long-id"] ~docv:"LONG_ID")

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

let alba_add_osd
      cfg_file
      tls_config

      (* for asd/kinetic *)
      host port
      (* for alba osd *)
      alba_osd_cfg_url prefix preset

      node_id
      to_json verbose attempts
  =
  let node_id = match node_id with
    | None ->  failwith "A node id is needed here"
    | Some n -> n
  in
  let t () =
    begin
      match host, port, alba_osd_cfg_url, prefix, preset with
      | Some host, Some port, None, None, None ->
         let conn_info = Networking2.make_conn_info [host] port tls_config in
         Discovery.get_kind Buffer_pool.default_buffer_pool conn_info >>=
           (function
             | None -> Lwt.fail_with "I don't think this is an OSD"
             | Some kind ->
                Lwt.return kind)
      | None, None, Some alba_osd_cfg_url, Some prefix, Some preset ->
         Alba_arakoon.config_from_url alba_osd_cfg_url >>= fun alba_osd_cfg ->
         Albamgr_client.with_client'
           ~attempts
           alba_osd_cfg
           ~tls_config
           ~tcp_keepalive:Tcp_keepalive2.default
           (fun mgr -> mgr # get_alba_id) >>= fun id ->
         Lwt.return Nsm_model.OsdInfo.(Alba { id;
                                              cfg = alba_osd_cfg;
                                              prefix;
                                              preset;
                                            })
      | _, _, _, _, _ ->
         failwith "incorrect combination of host, port alba_osd_cfg_url, prefix & preset specified"
    end >>= fun kind ->

    Osd_access.Osd_pool.factory
      tls_config
      Tcp_keepalive2.default
      Buffer_pool.osd_buffer_pool
      Alba_osd.make_client
      kind
    >>= fun (osd_client, closer) ->
    Lwt_log.info_f "long_id :%S" (osd_client # get_long_id) >>= fun () ->
    osd_client # get_disk_usage >>= fun (used, total) ->
    let other = "other?" in
    let osd_info =
      Nsm_model.OsdInfo.({
                            kind;
                            decommissioned = false;
                            node_id;
                            other;
                            total; used;
                            seen = [ Unix.gettimeofday (); ];
                            read = [];
                            write = [];
                            errors = [];
                          })
    in
    with_albamgr_client
      cfg_file ~attempts tls_config
      (fun client -> client # add_osd osd_info)
  in
  lwt_cmd_line ~to_json:false ~verbose t

let alba_osd_cfg_url =
  Arg.(value
       & opt (some url_converter) None
       & info ["alba-osd-config-url"]
              ~docv:"ALBA_OSD_CONFIG_URL"
              ~doc:"config url for alba mgr of the backend that should be added as osd")

let alba_add_osd_cmd =
  Term.(pure alba_add_osd
        $ alba_cfg_url
        $ tls_config
        $ Arg.(value
               & opt (some string) None
               & info ["host"; "h";]
                      ~docv:"HOST"
                      ~doc:"the host to connect with")
        $ Arg.(value
               & opt (some int) None
               & info ["port"; "p";]
                      ~docv:"PORT"
                      ~doc:"the port to connect with")
        $ alba_osd_cfg_url
        $ Arg.(value
               & opt (some string) None
               & info ["prefix"]
                      ~docv:"PREFIX"
                      ~doc:"prefix to use for the alba-osd")
        $ Arg.(value
               & opt (some string) None
               & info ["preset"]
                      ~docv:"PRESET"
                      ~doc:"preset to use for the alba-osd")
        $ (node_id None)
        $ to_json $ verbose
        $ attempts 1
  ),
  Term.info
    "add-osd"
    ~doc:("add osd to manager so it could be claimed later." ^
            "The long id is fetched from the device.")

let alba_update_osd
      alba_cfg_url tls_config long_id
      ips' port'
      alba_osd_mgr_cfg_url
      to_json verbose
  =
  let t () =
    with_albamgr_client
      alba_cfg_url tls_config
      ~attempts:1
      (fun mgr ->
       Option.lwt_map
         Alba_arakoon.config_from_url
         alba_osd_mgr_cfg_url >>= fun albamgr_cfg' ->
       mgr # update_osds
           [ (long_id,
              Albamgr_protocol.Protocol.Osd.Update.make
                ~ips' ?port'
                ?albamgr_cfg'
                ());
           ])
  in
  lwt_cmd_line_unit ~to_json ~verbose t

let alba_update_osd_cmd =
  Term.(pure alba_update_osd
        $ alba_cfg_url
        $ tls_config
        $ long_id
        $ Arg.(value
               & opt (list string) []
               & info ["ip"])
        $ Arg.(value
               & opt (some int) None
               & info ["port"])
        $ alba_osd_cfg_url
        $ to_json
        $ verbose
  ),
  Term.info
    "update-osd"
    ~doc:("update the osd info that is stored in the alba manager")

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
        $ long_id
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
        $ long_id
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
        $ long_id
        $ to_json $ verbose
  ),
  Term.info
    "purge-osd"
    ~doc:"tell this alba instance this osd is no longer available"

let cmds = [
    osd_multi_get_cmd;
    osd_range_cmd;
    alba_add_osd_cmd;
    alba_update_osd_cmd;
    alba_claim_osd_cmd;
    alba_decommission_osd_cmd;
    alba_purge_osd_cmd;
  ]
