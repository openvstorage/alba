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

module Config = struct
  type cluster_id = string [@@deriving show]
  (* TODO tls/ssl stuff *)
  type node_name = string [@@deriving show]
  type node_client_cfg = { ips : string list;
                           port : int; }
                           [@@deriving show]
  type t = cluster_id *  (node_name, node_client_cfg) Hashtbl.t

  let show ((cluster_id,  cfgs):t) =
    Printf.sprintf
      "cluster_id = %s , %s"
      cluster_id
      (*([%show : Tls.t option] tlso)*)
      ([%show : (string * node_client_cfg) list]
         (Hashtbl.fold (fun k v acc -> (k,v) :: acc) cfgs []))
  let pp formatter t =
    Format.pp_print_string formatter (show t)


  let to_buffer buf (cluster_id, cfgs) =
    let ser_version = 1 in Llio.int8_to buf ser_version;
                           Llio.string_to buf cluster_id;
                           Llio.hashtbl_to
                             Llio.string_to
                             (fun buf ncfg ->
                              Llio.list_to Llio.string_to buf ncfg.ips;
                              Llio.int_to buf ncfg.port)
                             buf
                             cfgs

  let from_buffer buf =
    let ser_version = Llio.int8_from buf in
    assert (ser_version = 1);
    let cluster_id = Llio.string_from buf in
    let cfgs =
      Llio.hashtbl_from
        (Llio.pair_from
           Llio.string_from
           (fun buf ->
            let ips = Llio.list_from Llio.string_from buf in
            let port = Llio.int_from buf in
            { ips; port; }))
        buf in
    (cluster_id, cfgs)


  let from_node_client_cfg cfg =
    { Arakoon_client_config.ips = cfg.ips;
      port = cfg.port }

  let to_arakoon_client_cfg tlso ((cluster_id, cfgs) : t) : Arakoon_client_config.t =
    {
      Arakoon_client_config.cluster_id;
      node_cfgs =
        Hashtbl.fold
          (fun name (cfg : node_client_cfg) acc ->
           (name, from_node_client_cfg cfg) :: acc)
          cfgs
          [];
      ssl_cfg = (Option.map Tls.to_ssl_cfg tlso);
    }
end

let _cfg_from_txt txt =
  let inifile = new Inifiles.inifile txt in
  let cluster_id =
    try
      let cids = inifile # getval "global" "cluster_id" in
      Scanf.sscanf cids "%s" (fun s -> s)
    with (Inifiles.Invalid_element _ ) -> failwith "config has no cluster_id" in
  let node_names = Ini.get inifile "global" "cluster" Ini.p_string_list Ini.required in
  let node_cfgs = Hashtbl.create 3 in
  List.iter
    (fun node_name ->
     let ips = Ini.get inifile node_name "ip" Ini.p_string_list Ini.required in
     let get_int x = Ini.get inifile node_name x Ini.p_int Ini.required in
     let port = get_int "client_port" in
     let cfg = Config.{ ips; port } in
     Hashtbl.add node_cfgs node_name cfg)
    node_names;
  (cluster_id, node_cfgs) |> Lwt.return

let config_from_url url = Etcd.retrieve_cfg _cfg_from_txt url
