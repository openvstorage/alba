(*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

open Prelude
open Lwt
open Cmdliner
open Cli_common

let preset_name p =
  Arg.(required
       & pos p (some string) None
       & info []
         ~docv:"PRESET_NAME"
         ~doc:"name of the preset")

let alba_create_preset
    cfg_file preset_name
    to_json
  =
  let t () =
    Lwt_io.read Lwt_io.stdin >>= fun txt ->
    let json = Yojson.Safe.from_string txt in
    let preset' =
      match Alba_json.Preset.of_yojson json with
      | `Error s -> failwith s
      | `Ok p -> p
    in
    Alba_json.Preset.to_preset
      preset' >>= fun preset ->
    let open Albamgr_protocol.Protocol in
    Lwt_log.debug_f "Storing preset %s" (Preset.show preset) >>= fun () ->
    Albamgr_client.with_client'
      (Arakoon_config.from_config_file cfg_file)
      (fun client ->
         client # create_preset
           preset_name
           preset)
  in
  lwt_cmd_line_unit to_json t

let alba_create_preset_cmd =
  Term.(pure alba_create_preset
        $ alba_cfg_file
        $ preset_name 0
        $ to_json),
  Term.info
    "create-preset"
    ~doc:"create a new preset. the preset is read from stdin as json, pls have a look at cfg/preset.json for more details."

let alba_update_preset
      cfg_file preset_name
      to_json
  =
  let t () =
    Lwt_io.read Lwt_io.stdin >>= fun txt ->
    let json = Yojson.Safe.from_string txt in
    let open Albamgr_protocol.Protocol in
    let preset_updates =
      match Preset.Update.of_yojson json with
      | `Error s -> failwith s
      | `Ok p -> p
    in
    Albamgr_client.with_client'
      (Arakoon_config.from_config_file cfg_file)
      (fun client ->
         client # update_preset
           preset_name
           preset_updates)
  in
  lwt_cmd_line_unit to_json t

let alba_update_preset_cmd =
  Term.(pure alba_update_preset
        $ alba_cfg_file
        $ preset_name 0
        $ to_json),
  Term.info
    "update-preset"
    ~doc:"update an existing preset. the preset is read from stdin as json, pls have a look at cfg/update_preset.json for more details."

let alba_preset_set_default cfg_file preset_name to_json =
  let t () =
    let open Albamgr_protocol.Protocol in
    Albamgr_client.with_client'
      (Arakoon_config.from_config_file cfg_file)
      (fun client ->
         client # set_default_preset preset_name)
  in
  lwt_cmd_line_unit to_json t

let alba_preset_set_default_cmd =
  Term.(pure alba_preset_set_default
        $ alba_cfg_file
        $ preset_name 0
        $ to_json),
  Term.info "preset-set-default" ~doc:"make the specified preset the default preset"

let alba_add_osds_to_preset cfg_file preset_name osd_ids to_json =
  let t () =
    let open Albamgr_protocol.Protocol in
    Albamgr_client.with_client'
      (Arakoon_config.from_config_file cfg_file)
      (fun client ->
         client # add_osds_to_preset ~preset_name ~osd_ids)
  in
  lwt_cmd_line_unit to_json t

let alba_add_osds_to_preset_cmd =
  Term.(pure alba_add_osds_to_preset
        $ alba_cfg_file
        $ preset_name 0
        $ Arg.(value
               & opt_all int32 []
               & info
                 ["osd-id"]
                 ~docv:"OSD_IDS"
                 ~doc:"the osds to be added to this preset")
        $ to_json),
  Term.info "add-osds-to-preset" ~doc:"add some osds to the specified preset"

let alba_delete_preset cfg_file preset_name to_json =
  let t () =
    let open Albamgr_protocol.Protocol in
    Albamgr_client.with_client'
      (Arakoon_config.from_config_file cfg_file)
      (fun client ->
         client # delete_preset preset_name)
  in
  lwt_cmd_line_unit to_json t

let alba_delete_preset_cmd =
  Term.(pure alba_delete_preset
        $ alba_cfg_file
        $ preset_name 0
        $ to_json),
  Term.info "delete-preset" ~doc:"delete the specified preset"

let alba_list_presets cfg_file to_json =
  let t () =
    let open Albamgr_protocol.Protocol in
    Albamgr_client.with_client'
      (Arakoon_config.from_config_file cfg_file)
      (fun client ->
         client # list_all_presets ()) >>= fun (cnt, presets) ->
    if to_json
    then begin
      let res = List.map Alba_json.Preset.make presets in
      print_result res Alba_json.Preset.t_list_to_yojson
    end else
      Lwt_io.printlf
        "Found %i presets: %s"
        cnt
        ([%show : (Preset.name * Preset.t * bool * bool) list] presets)
  in
  lwt_cmd_line to_json t

let alba_list_presets_cmd =
  Term.(pure alba_list_presets $ alba_cfg_file $ to_json),
  Term.info "list-presets" ~doc:"list the presets available in the albamgr"

let cmds = [
  alba_create_preset_cmd;
  alba_update_preset_cmd;
  alba_preset_set_default_cmd;
  alba_add_osds_to_preset_cmd;
  alba_delete_preset_cmd;
  alba_list_presets_cmd;
]
