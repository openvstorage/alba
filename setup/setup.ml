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

open Prul

module Config = struct
  let env_or_default_generic (f:string -> 'a) (x:string) (y:'a) =
    try
      f (Sys.getenv x)
    with Not_found -> y

  let env_or_default = env_or_default_generic (fun x -> x)
  type t = {
      home : string;
      workspace : string;
      arakoon_home: string;
      arakoon_bin : string;
      arakoon_189_bin: string;
      arakoon_path : string;

      alba_home : string;
      alba_base_path : string;
      alba_bin : string;
      alba_plugin_path : string;
      alba_06_bin : string;
      alba_06_plugin_path :string;
      license_file : string;
      tls : bool;

      alba_rdma : string option; (* ip of the rdma capable nic *)
      local_nodeid_prefix : string;
      n_osds : int;

      monitoring_file : string ;

      voldrv_test : string;
      voldrv_backend_test : string;
      failure_tester : string;
      etcd_home:string;
      etcd : ((string * int) list * string) option;

      alba_blob_io_engine: string list;
    }

  let make ?workspace () =
    let home = Sys.getenv "HOME" in
    let workspace =
      match workspace with
      | None -> env_or_default "WORKSPACE" (Unix.getcwd ())
      | Some w -> w
    in
    let arakoon_home = env_or_default "ARAKOON_HOME" (home ^ "/workspace/ARAKOON/arakoon") in
    let arakoon_bin = env_or_default "ARAKOON_BIN" (arakoon_home ^ "/arakoon.native") in
    let arakoon_189_bin = env_or_default "ARAKOON_189_BIN" "/usr/bin/arakoon" in
    let arakoon_path = workspace ^ "/tmp/arakoon" in

    let alba_home = env_or_default "ALBA_HOME" workspace in
    let alba_base_path = workspace ^ "/tmp/alba" in

    let alba_bin    = env_or_default "ALBA_BIN" (alba_home  ^ "/ocaml/alba.native") in
    let alba_plugin_path = env_or_default "ALBA_PLUGIN_HOME" (alba_home ^ "/ocaml") in
    let alba_06_bin = env_or_default "ALBA_06"  "/usr/bin/alba.0.6" in
    let alba_06_plugin_path = env_or_default "ALBA_06_PLUGIN_PATH" "/usr/lib/alba" in
    let license_file = alba_home ^ "/bin/0.6/community_license" in
    let failure_tester = alba_home ^ "/ocaml/disk_failure_tests.native" in

    let monitoring_file = workspace ^ "/tmp/alba/monitor.txt" in

    let local_nodeid_prefix = Printf.sprintf "%08x" (Random.bits ()) in
    (* let asd_path_t = env_or_default "ALBA_ASD_PATH_T" (alba_base_path ^ "/asd/%02i") in *)

    let voldrv_test = env_or_default
                      "VOLDRV_TEST"
                      (home ^ "/workspace/VOLDRV/volumedriver_test") in
    let voldrv_backend_test = env_or_default
                              "VOLDRV_BACKEND_TEST"
                              (home ^ "/workspace/VOLDRV/backend_test") in
    let n_osds = 12 in
    let etcd_home = env_or_default "ETCD_HOME" (workspace ^ "/tmp/etcd") in
    let etcd =
      try
        let url = Sys.getenv "ALBA_ETCD" in
        let host,port,path =
          Scanf.sscanf url "%s@:%i/%s" (fun h i p -> h,i, "/" ^ p)
        in
        let peers = [host,port] in
        Some (peers, path)
      with Not_found -> None
    in
    let tls =
      let v = env_or_default "ALBA_TLS" "false" in
      Scanf.sscanf v "%b" (fun x -> x)
    in
    let alba_rdma = env_or_default_generic (fun x -> Some x) "ALBA_RDMA" None in
    let alba_blob_io_engine =
      env_or_default_generic
        (fun x ->
          Str.split (Str.regexp ",") x
        )
        "ALBA_BLOB_IO_ENGINE" ["Pure"]
    in

    {
      home;
      workspace;
      arakoon_home;
      arakoon_bin;
      arakoon_189_bin;
      arakoon_path;

      alba_home;
      alba_base_path;
      alba_bin;
      alba_plugin_path;
      alba_06_bin;
      alba_06_plugin_path;
      license_file;
      tls;
      alba_rdma;
      local_nodeid_prefix;
      n_osds;

      monitoring_file;

      voldrv_test;
      voldrv_backend_test;
      failure_tester;
      etcd_home;
      etcd;
      alba_blob_io_engine;
    }

  let default = make ()

  let generate_serial =
    let serial = ref 0 in
    fun () ->
       let r = !serial in
       let () = incr serial in
       Printf.sprintf "%i" r


end
open Config

let make_ca (cfg:Config.t) =
  let arakoon_path = cfg.arakoon_path in
  let cacert_req = arakoon_path ^ "/cacert-req.pem" in
  Printf.printf "make %s\n%!" cacert_req ;
  let key = arakoon_path ^ "/cacert.key" in

  let subject = "'/C=BE/ST=Vl-Br/L=Leuven/O=openvstorage.com/OU=AlbaTest/CN=AlbaTest CA'" in
  ["openssl"; "req";"-new"; "-nodes";
   "-out";    cacert_req;
   "-keyout"; key;
   "-subj"; subject;
  ]
  |> String.concat " "
  |> Shell.cmd;


  Printf.printf "self sign \n%!" ;
  (* Self sign CA CSR *)
  let cacert = arakoon_path ^ "/cacert.pem" in
  ["openssl";"x509";
   "-signkey"; key;
   "-req"; "-in"  ; cacert_req;
   "-out" ; cacert;
  ] |> String.concat " " |> Shell.cmd;

  "rm " ^ cacert_req |> Shell.cmd

let make_cert ?(cfg=Config.default) path name =
  let subject =
    Printf.sprintf "'/C=BE/ST=Vl-BR/L=Leuven/O=openvstorage.com/OU=AlbaTest/CN=%s'"
                   name
  in
  (* req *)
  let req = Printf.sprintf "%s/%s-req.pem" path name in
  let key = Printf.sprintf "%s/%s.key" path name in
  ["openssl";"req";
   "-out"; req;
   "-new"; "-nodes";
   "-keyout"; key;
   "-subj" ; subject;
  ] |> String.concat " " |> Shell.cmd;

  (* sign *)
  let arakoon_path = cfg.arakoon_path in
  let cacert = arakoon_path ^ "/cacert.pem" in
  let name_pem = Printf.sprintf "%s/%s.pem" path name in
  ["openssl"; "x509"; "-req"; "-in" ; req;
   "-CA"; cacert;
   "-CAkey"; arakoon_path ^ "/cacert.key";
   "-out"; name_pem;
   "-CAcreateserial";"-CAserial" ; arakoon_path ^ "/cacert-serial.seq"
  ] |> String.concat " " |> Shell.cmd;

  "rm " ^ req |> Shell.cmd;

  (* verify *)
  ["openssl"; "verify";
   "-CAfile"; cacert;
   name_pem
  ] |> String.concat " " |> Shell.cmd


let _arakoon_cmd_line ?(cfg=Config.default) x =
  String.concat " " (cfg.arakoon_bin :: x) |> Shell.cmd

let _get_client_tls ?(cfg=Config.default) ()=
  let arakoon_path = cfg.arakoon_path in
  let cacert = arakoon_path ^ "/cacert.pem" in
  let pem    = arakoon_path ^ "/my_client/my_client.pem" in
  let key    = arakoon_path ^ "/my_client/my_client.key" in
  (cacert,pem,key)




class arakoon ?(cfg=Config.default) cluster_id nodes base_port etcd =
  let arakoon_path = cfg.arakoon_path in
  let cluster_path = arakoon_path ^ "/" ^ cluster_id in
  let _extend_tls cmd =
    let cacert,my_client_pem,my_client_key = _get_client_tls () in
    cmd @ [
        "-tls-ca-cert"; cacert;
        "-tls-cert"; my_client_pem;
        "-tls-key"; my_client_key;
      ]
  in
  let _make_cfg_value () =
    let buf = Buffer.create 128 in
    let w x = Printf.ksprintf (fun s -> Buffer.add_string buf s) (x ^^ "\n") in
    w "[global]";
    w "cluster = %s" (String.concat ", " nodes);
    w "cluster_id = %s" cluster_id;
    w "plugins = albamgr_plugin nsm_host_plugin";
    w "";
    if cfg.tls
    then
      begin
        w "tls_ca_cert = %s/cacert.pem" cfg.arakoon_path;
        w "tls_service = true";
        w "tls_service_validate_peer = false";
        w "";
      end;
    List.iteri
      (fun i node ->
       w "[%s]" node;
       w "ip = 127.0.0.1";
       w "client_port = %i" (base_port + i);
       w "messaging_port = %i" (base_port + i + 10);
       let home = cfg.arakoon_path ^ "/" ^ cluster_id ^ "/" ^ node in
       w "home = %s" home;
       w "log_level = debug";
       w "fsync = false";
       w "";
       if cfg.tls then
         begin
           w "tls_cert = %s/%s.pem" home node;
           w "tls_key =  %s/%s.key" home node;
           w "";
         end;

      )
      nodes;
    Buffer.contents buf
  in
  let config_persister, cfg_url =
    match etcd with
    | None ->
       let cfg_file = arakoon_path ^ "/" ^ cluster_id ^ ".ini" in
       let persister () =
         let cfg_txt = _make_cfg_value () in
         let oc = open_out cfg_file in
         let () = output_string oc cfg_txt in
         close_out oc
       in
       let url = Url.File cfg_file in
       persister, url
    | Some etcd ->
       let key = Printf.sprintf "/arakoons/%s" cluster_id in
       let persister () =
         let value = _make_cfg_value () in
         Etcdctl.set etcd key value
       in
       let url = Etcdctl.url etcd key in
       persister, url
  in
  object (self : # component)
    val mutable _binary = cfg.arakoon_bin
    val mutable _plugin_path = cfg.alba_plugin_path

    method to_arakoon_189  =
      _binary <- cfg.arakoon_189_bin;
      _plugin_path <- cfg.alba_06_plugin_path

    method config_url : Url.t = cfg_url
    method cluster_id = cluster_id

    method link_plugins node =
      let dir_path = cluster_path ^ "/" ^ node in
      Shell.mkdir dir_path;
      Printf.sprintf
        "ln -fs %s/nsm_host_plugin.cmxs %s/nsm_host_plugin.cmxs"
        _plugin_path dir_path |> Shell.cmd;
      Printf.sprintf
        "ln -fs %s/albamgr_plugin.cmxs %s/albamgr_plugin.cmxs"
        _plugin_path dir_path |> Shell.cmd;
      if cfg.tls then make_cert dir_path node



    method persist_cluster_config =
      config_persister ()


    method persist_config =
      List.iter (self # link_plugins) nodes;
      self # persist_cluster_config

    method start_node node =
      [_binary;
       "--node"; node;
       "-config"; Url.canonical cfg_url
      ] |> Shell.detach

    method start =
      List.iter (self # start_node) nodes

    method stop_node name =
      let pid_line = ["pgrep -a arakoon"; "| grep "; name ] |> Shell.cmd_with_capture in
      let pid = Scanf.sscanf pid_line " %i " (fun i -> i) in
      Printf.sprintf "kill %i" pid |> Shell.cmd

    method stop =
      List.iter (self # stop_node) nodes

    method remove_dirs =
      List.iter
        (fun node ->
         let rm = Printf.sprintf "rm -rf %s/%s" cluster_path node in
         let _ = Shell.cmd rm in
         ()
        )
        nodes

    method who_master () : string =
      let line = [cfg.arakoon_bin; "--who-master";"-config"; Url.canonical cfg_url] in
      let line' = if cfg.tls
                  then _extend_tls line
                  else line
      in
      Shell.cmd_with_capture line'

    method wait_for_master ?(max=20) () : string =

      let step () =
        try
          let r = self # who_master () in
          Some r
        with _ -> None
      in
      let rec loop n =
        if n = 0
        then failwith "No_master"
        else
          let mo = step () in
          match mo with
          | None ->
             let () = Printf.printf "%i\n%!" n; Unix.sleep 1 in
             loop (n-1)
          | Some master -> master
      in loop max
end

type tls_client =
  { ca_cert : string;
    creds : string * string;
  } [@@ deriving yojson]

let make_tls_client (cfg:Config.t) =
  if cfg.tls
  then
    let arakoon_path = cfg.arakoon_path in
    let ca_cert = arakoon_path ^ "/cacert.pem" in
    let my_client_pem = arakoon_path ^ "/my_client/my_client.pem" in
    let my_client_key = arakoon_path ^ "/my_client/my_client.key" in
    Some { ca_cert; creds = (my_client_pem, my_client_key)}
  else None

module Proxy_cfg =
  struct
    type proxy_cfg_06 =
      { port: int;
        albamgr_cfg_file : Url.t;
        log_level : string;
        fragment_cache_dir : string;
        manifest_cache_size : int;
        fragment_cache_size : int;
        tls_client : tls_client option;
        use_fadvise: bool [@default true];
      } [@@deriving yojson]

    type fragment_cache_cfg =
      | None' [@name "none"]
      | Local of local_fragment_cache [@name "local"]
      | Alba of alba_fragment_cache [@name "alba"]
    [@@deriving yojson]

     and local_fragment_cache = {
         path : string;
         max_size : int;
         cache_on_read : bool;
         cache_on_write : bool;
       } [@@deriving yojson]

     and alba_fragment_cache = {
         albamgr_cfg_url : string;
         bucket_strategy : bucket_strategy;
         manifest_cache_size : int;
         cache_on_read_ : bool [@key "cache_on_read"];
         cache_on_write_ : bool [@key "cache_on_write"];
       } [@@deriving yojson]

     and bucket_strategy =
       | OneOnOne of bucket_strategy_one_on_one [@name "1-to-1"]
       [@@deriving yojson]

     and bucket_strategy_one_on_one = {
         prefix : string;
         preset : string;
       } [@@deriving yojson]

    type proxy_cfg =
      { ips : string list option [@default None];
        transport : string option [@default None];
        port : int;
        albamgr_cfg_url : Url.t;
        log_level : string;
        manifest_cache_size : int;
        fragment_cache : fragment_cache_cfg;
        tls_client : tls_client option;
        use_fadvise : bool [@default true];
      } [@@deriving yojson]

    type t =
      | Old of proxy_cfg_06
      | New of proxy_cfg

    let to_yojson = function
      | Old t -> proxy_cfg_06_to_yojson t
      | New t -> proxy_cfg_to_yojson t

    let make_06 ?fragment_cache
                ?ip
                ?transport
                id abm_cfg_url base tls_client port =
      Old
        { port = port + id;
          albamgr_cfg_file = abm_cfg_url;
          log_level = "debug";
          fragment_cache_dir  = base ^ "/fragment_cache";
          manifest_cache_size = 100 * 1000;
          fragment_cache_size = 100 * 1000 * 1000;
          tls_client;
          use_fadvise = true;
        }

    let make ?fragment_cache
             ?ip
             ?transport
             id albamgr_cfg_url base tls_client port =
      let fragment_cache =
        match fragment_cache with
        | Some x -> x
        | None -> Local {
                      path = base ^ "/fragment_cache";
                      max_size = 100 * 1000 * 1000;
                      cache_on_read = true; cache_on_write = true;
                    }
      in
      let ips = match ip with
        | None -> None
        | Some ip -> Some [ip]
      in
      New
        { ips ;
          transport;
          port = port + id;
          albamgr_cfg_url;
          log_level = "debug";
          fragment_cache;
          manifest_cache_size = 100 * 1000;
          tls_client;
          use_fadvise = true;
        }

    let port = function
      | Old x -> x.port
      | New x -> x.port
  end

let _alba_extend_tls ?(cfg=Config.default) cmd =
  let arakoon_path = cfg.arakoon_path in
  let cacert = arakoon_path ^ "/cacert.pem" in
  let my_client_pem = arakoon_path ^ "/my_client/my_client.pem" in
  let my_client_key = arakoon_path ^ "/my_client/my_client.key" in
  cmd @ [Printf.sprintf
           "--tls=%s,%s,%s" cacert my_client_pem my_client_key]

let _alba_cmd ~cfg ~cwd ~ignore_tls x =
  let maybe_extend_tls cmd =
    if not ignore_tls && cfg.tls
    then
      begin
        _alba_extend_tls cmd
      end
    else cmd
  in
  let cmd = (cfg.alba_bin :: x) in
  let cmd1 = match cwd with
    | Some dir -> "cd":: dir ::"&&":: cmd
    | None -> cmd
  in
  cmd1
  |> maybe_extend_tls

let _alba_cmd_line ?(cfg=Config.default) ?cwd ?(ignore_tls=false) x =
  _alba_cmd ~cfg ~cwd ~ignore_tls x
  |> String.concat " "
  |> Shell.cmd ~ignore_rc:false


let suppress_tags tags = function
  | `Assoc xs ->
     let xs' =
       List.filter
         (fun (tag, value ) ->
          not (List.mem tag tags)
          && value <> `Null
         ) xs in
     `Assoc xs'
  | _ -> failwith "unexpected json"

class proxy ?fragment_cache ?ip ?transport
            id cfg alba_bin (abm_cfg_url:Url.t) etcd ~v06_proxy port =
  let proxy_base = Printf.sprintf "%s/proxies/%02i" cfg.alba_base_path id in
  let tls_client = make_tls_client cfg in
  let p_cfg =
    (if v06_proxy
     then Proxy_cfg.make_06
     else Proxy_cfg.make)
      id abm_cfg_url proxy_base tls_client
      port ?fragment_cache ?ip ?transport
  in
  let config_persister, cfg_url =
    match etcd with
    | None ->
       let p_cfg_file = proxy_base ^ "/proxy.cfg" in
       let cfg_url = Url.File p_cfg_file in
       let persister cfg =
         let oc = open_out p_cfg_file in
         let json = Proxy_cfg.to_yojson cfg in
         let json' = suppress_tags [] json in
         Yojson.Safe.pretty_to_channel oc json' ;
         close_out oc
       in
       persister, cfg_url
    | Some etcd ->
       let key = Printf.sprintf "/proxies/%02i" id in
       let persister cfg =
         let json = Proxy_cfg.to_yojson cfg in
         let value = Yojson.Safe.pretty_to_string json in
         Etcdctl.set etcd key value
       in
       let url = Etcdctl.url etcd key in
       persister, url
  in
  let proxy_cmd_line_with_capture cmd =
    let _ip = match ip with
      | None    -> "127.0.0.1"
      | Some ip -> ip
    in
    let _transport = match transport with
      | None -> "tcp"
      | Some t -> t
    in
    cfg.alba_bin ::
    List.append
      cmd
      [ "-h"; _ip;
        "-p"; Proxy_cfg.port p_cfg |> string_of_int;
        "-t"; _transport
      ]
    |> Shell.cmd_with_capture
  in
  let proxy_cmd_line cmd = proxy_cmd_line_with_capture cmd |> ignore in
  object (self : # component)

  method persist_config :unit =
    Shell.mkdir proxy_base;
    config_persister p_cfg

  method log_file = Printf.sprintf "%s/proxy.out" proxy_base

  method start_cmd = [alba_bin; "proxy-start"; "--config"; Url.canonical cfg_url]
  method start =
    let out = self # log_file in
    let () =
      let open Proxy_cfg in
      match p_cfg with
      | Old { fragment_cache_dir; _ } -> Shell.mkdir fragment_cache_dir
      | New x ->
         match x.fragment_cache with
         | Alba _
         | None' -> ()
         | Local { path; _ } -> Shell.mkdir path
    in
    self # start_cmd
    |> Shell.detach ~out

  method stop =
    (* can't use fuser for rdma based servers *)
    let start_line = String.concat " " (self # start_cmd) in
    Printf.sprintf "pkill -f '%s'" start_line
    |> Shell.cmd ~ignore_rc:true

  method upload_object namespace file name =
    ["proxy-upload-object";
     namespace; file ; name ]
    |> proxy_cmd_line

  method download_object namespace name file =
    ["proxy-download-object";
     namespace; name ;file ]
    |> proxy_cmd_line

  method delete_object namespace name =
    ["proxy-delete-object"; namespace; name ] |> proxy_cmd_line

  method list_objects namespace =
    [ "proxy-list-objects"; namespace ] |> proxy_cmd_line_with_capture

  method create_namespace name =
    [ "proxy-create-namespace"; name ] |> proxy_cmd_line

  method delete_namespace name =
    ["proxy-delete-namespace"; name] |> proxy_cmd_line


  method list_namespaces =
    [ "proxy-list-namespaces" ] |> proxy_cmd_line_with_capture

  method cmd_line cmd = proxy_cmd_line cmd

end

type maintenance_cfg = {
    albamgr_cfg_url : Url.t;
    log_level : string;
    tls_client : tls_client option;
  } [@@deriving yojson]

let make_maintenance_config abm_cfg_url tls_client =
  { albamgr_cfg_url = abm_cfg_url;
    log_level = "debug";
    tls_client ;
  }

class maintenance id cfg (abm_cfg_url:Url.t) etcd =
  let maintenance_base =
    Printf.sprintf "%s/maintenance/%02i" cfg.alba_base_path id
  in
  let maintenance_abm_cfg_file = maintenance_base ^ "/abm.ini" in
  let maintenance_abm_cfg_url =
    match etcd with
    | None      -> Url.File maintenance_abm_cfg_file
    | Some etcd -> abm_cfg_url
  in
  let tls_client = make_tls_client cfg in
  let m_cfg = make_maintenance_config maintenance_abm_cfg_url tls_client in

  let config_persister, cfg_url = match etcd with
    | None ->
       let m_cfg_file = maintenance_base ^ "/maintenance.cfg" in
       let url = Url.File m_cfg_file in
       let persister cfg =
         let oc = open_out m_cfg_file in
         let json = maintenance_cfg_to_yojson cfg in
         Yojson.Safe.pretty_to_channel oc json;
         close_out oc
       in
       persister, url
    | Some etcd ->
       let key = Printf.sprintf "/maintenance/%02i_cfg" id in
       let url = Etcdctl.url etcd key in
       let persister cfg =
         let json = maintenance_cfg_to_yojson cfg in
         let value = Yojson.Safe.pretty_to_string json in
         Etcdctl.set etcd key value
       in

       persister, url
  in
  object
    method abm_config_url : Url.t = maintenance_abm_cfg_url

    method write_config_file : unit =
      "mkdir -p " ^ maintenance_base |> Shell.cmd;
      let () =
        match etcd with
        | None ->
           begin
             match abm_cfg_url with
             | Url.File abm_cfg_file -> Shell.cp abm_cfg_file maintenance_abm_cfg_file
             | _ -> failwith "not supported"
           end
        | _ -> ()
      in
      config_persister m_cfg

    method start =
      let out = Printf.sprintf "%s/maintenance.out" maintenance_base in
      [cfg.alba_bin; "maintenance"; "--config"; Url.canonical cfg_url]
      |> Shell.detach ~out

    method signal s=
      let pid_line = ["pgrep -a alba"; "| grep 'maintenance' " ]
                     |> Shell.cmd_with_capture
      in
      let pid = Scanf.sscanf pid_line " %i " (fun i -> i) in
      Printf.sprintf "kill -s %s %i" s pid |> Shell.cmd


end



type tls = { cert:string; key:string; port : int} [@@ deriving yojson]
type blob_io_engine =
  | Pure
  | GioExecFile of string [@@ deriving yojson, show]

let blob_io_engine_from_cfg = function
  | ["Pure"] -> Pure
  | ["GioExecFile"; x] -> GioExecFile x
  | x -> failwith (Printf.sprintf "unknown blob io engine :%s" ([% show: string list] x))

type asd_cfg = {
    node_id: string;
    home : string;
    log_level : string;
    ips : string list;
    port : int option;
    asd_id : string;
    limit : int;
    __sync_dont_use: bool;
    multicast: float option;
    tls: tls option;
    transport : string option;
    __warranty_void__write_blobs  : (bool option [@default None]);
    use_fadvise  : (bool [@default true]);
    use_fallocate: (bool [@default true]);
    blob_io_engine : (blob_io_engine [@default Pure]);

  }[@@deriving yojson]

let make_asd_config
      ?write_blobs
      ?(use_fadvise = true)
      ?(use_fallocate = true)
      ?transport
      ?ip
      node_id asd_id home port tls
      blob_io_engine
  =
  let ips = match ip with
    | None -> []
    | Some ip -> [ip]
  in
  {node_id;
   asd_id;
   home;
   port;
   ips;
   log_level = "debug";
   limit= 99;
   __sync_dont_use = false;
   multicast = Some 10.0;
   tls;
   transport;
   __warranty_void__write_blobs   = write_blobs;
   use_fadvise;
   use_fallocate;
   blob_io_engine;
  }




class asd ?write_blobs ?transport ?ip
          node_id asd_id alba_bin arakoon_path home port ~etcd tls
          (blob_io_engine: blob_io_engine)
  =
  let use_tls = tls <> None in
  let asd_cfg = make_asd_config
                  ?write_blobs ?transport ?ip
                  node_id asd_id home port tls
                  blob_io_engine
  in

  let config_persister, cfg_url =
    match etcd with
    | None ->
       let persister cfg =
         let file = home ^ "/cfg.json" in
         let oc = open_out file in
         let json = asd_cfg_to_yojson asd_cfg in
         let json' =
           if use_tls
           then json
           else suppress_tags ["multicast"] json
         in
         Yojson.Safe.pretty_to_channel oc json' ;
         close_out oc
       and url = Url.File (home ^ "/cfg.json") in
       persister,url
    | Some etcd ->
       let key = Printf.sprintf "/asds/%s/%s" node_id asd_id  in
       let persister cfg =
         let json = asd_cfg_to_yojson asd_cfg in
         let json' =
           if use_tls
           then json
           else suppress_tags ["multicast"] json
         in
         let value = Yojson.Safe.to_string json' in
         Etcdctl.set etcd key value
       in
       let url = Etcdctl.url etcd key in
       persister, url

  in
  object(self : # component)
    method long_id = asd_cfg.asd_id

    method config_url = cfg_url

    method tls = tls

    method persist_config =
      "mkdir -p " ^ home |> Shell.cmd;
      if use_tls
      then
        begin
        let base = Printf.sprintf "%s/%s" arakoon_path asd_id in
        "mkdir -p " ^ base |> Shell.cmd;
        make_cert base asd_id;
        end;
      config_persister asd_cfg

    method start_cmd = [alba_bin; "asd-start"; "--config"; Url.canonical cfg_url]

    method start =
      let out = Printf.sprintf "%s/%s.out" home asd_id in
      self # start_cmd
      |> Shell.detach ~out;

    method stop =
      let start_line = String.concat " " self # start_cmd in
      Printf.sprintf "pkill %s -f '%s'"
                     (if Random.bool ()
                      then "-9"
                      else "")
                     start_line
      |> Shell.cmd ~ignore_rc:true

    method private build_remote_cli ?(json=true) what  =
      let ip = match ip with
        | None -> "127.0.0.1"
        | Some ip -> ip
      in
      let transport = match transport with
        | None -> "tcp"
        | Some t -> t
      in
      let p = match tls with
        | Some tls -> tls.port
        | None -> begin match port with | Some p -> p | None -> failwith "bad config" end
      in
      let cmd0 = [ alba_bin;]
                 @ what
                 @ ["-h"; ip ;
                    "-p"; string_of_int p;
                    "-t"; transport;
                   ]
      in
      let cmd1 = if use_tls then _alba_extend_tls cmd0 else cmd0 in
      let cmd2 = if json then cmd1 @ ["--to-json"] else cmd1 in
      cmd2

    method get_remote_version =
      let cmd = self # build_remote_cli ["asd-get-version"] ~json:false in
      cmd |> Shell.cmd_with_capture

    method get_statistics =
      let cmd = self # build_remote_cli ["asd-statistics"] in
      cmd |> Shell.cmd_with_capture

    method set k v =
      let cmd = self # build_remote_cli ["asd-set";k;v] ~json:false in
      cmd |> String.concat " " |> Shell.cmd

    method get k =
      let cmd = self # build_remote_cli ["asd-multi-get"; k] ~json:false in
      cmd |> Shell.cmd_with_capture
end

class etcd host port home =
  object(self : #component)
    method persist_config =
      "mkdir -p " ^ home |> Shell.cmd

    method start =
      (* ETCD_LISTEN_CLIENT_URLS=http://127.0.0.1:5000 \
           ./etcd -advertise-client-urls=http://127.0.0.1:5000
       *)
      let out = home ^ "/etcd.out" in
      let url = Printf.sprintf "http://%s:%i" host port in
      let var = Printf.sprintf "ETCD_LISTEN_CLIENT_URLS=%s" url in
      let advertise = Printf.sprintf "-advertise-client-urls=%s" url in
      ["etcd"; advertise;"-data-dir";home] |> Shell.detach ~pre:[var] ~out;
      Unix.sleep 1; ()

    method stop =
      Printf.sprintf "fuser -k -n tcp %i" port
      |> Shell.cmd ~ignore_rc:true
  end

module Deployment = struct
  type t = {
      cfg : Config.t;
      abm : arakoon;
      nsm : arakoon;
      proxy : proxy;
      maintenance : maintenance;
      osds : asd array;
      etcd: etcd option;
    }

  let nsm_host_register t : unit =
    let cfg_url = t.nsm # config_url in
    let cmd = ["add-nsm-host"; Url.canonical cfg_url ;
               "--config" ; t.abm # config_url |> Url.canonical ]
    in
    _alba_cmd_line cmd



  let make_osds
        ?(base_port=8000)
        ?write_blobs
        ?transport ?ip
        n local_nodeid_prefix base_path arakoon_path alba_bin
        ~etcd
        (tls:bool)
        (blob_io_engine: blob_io_engine)
    =
    let rec loop asds j =
      if j = n
      then List.rev asds |> Array.of_list
      else
        begin
          let port = base_port + j in
          let node_id = j lsr 2 in
          let node_id_s = Printf.sprintf "%s_%i" local_nodeid_prefix node_id in
          let asd_id = Printf.sprintf "%04i_%02i_%s" port node_id local_nodeid_prefix in
          let home = base_path ^ (Printf.sprintf "/asd/%02i" j) in
          let tls_cfg =
            if tls
            then
              begin
                let port = port + 500 in
                let base = Printf.sprintf "%s/%s" arakoon_path asd_id in
                Some { cert = Printf.sprintf "%s/%s.pem" base asd_id ;
                       key  = Printf.sprintf "%s/%s.key" base asd_id ;
                       port ;
                     }
              end
            else None
          in
          let asd = new asd
                        ?write_blobs
                        ?transport
                        ?ip
                        node_id_s asd_id
                        alba_bin
                        arakoon_path
                        home (Some port) ~etcd
                        tls_cfg
                        blob_io_engine
          in
          loop (asd :: asds) (j+1)
        end
    in
    loop [] 0

  let make_default
        ?(cfg = Config.default) ?(base_port=4000)
        ?write_blobs ?fragment_cache () =
    let abm =
      let id = "abm"
      and nodes = ["abm_0"; "abm_1"; "abm_2"] in
      new arakoon ~cfg id nodes base_port cfg.etcd
    in
    let nsm =
      let id = "nsm"
      and nodes = ["nsm_0";"nsm_1"; "nsm_2"] in
      new arakoon ~cfg id nodes (base_port + 100) cfg.etcd
    in
    let etcd = match cfg.etcd with
      | None -> None
      | Some (peers,prefix) ->
         begin
           let (host,port) = List.hd peers in
           let etcd = new etcd host port cfg.etcd_home in
           Some etcd
         end
    in
    let transport,ip =
      match cfg.alba_rdma with
      | None -> None,None
      | Some ip ->Some "rdma", Some ip
    in

    let proxy = new proxy
                    0 cfg cfg.alba_bin
                    (abm # config_url) cfg.etcd ~v06_proxy:false
                    (base_port * 2 + 2000)
                    ?fragment_cache ?transport ?ip
    in
    let maintenance = new maintenance 0 cfg  (abm # config_url) cfg.etcd in
    let osds = make_osds ~base_port:(base_port*2)
                         ?write_blobs
                         ?transport
                         ?ip
                         cfg.n_osds
                         cfg.local_nodeid_prefix
                         cfg.alba_base_path
                         cfg.arakoon_path
                         cfg.alba_bin
                         ~etcd:cfg.etcd
                         cfg.tls
                         (blob_io_engine_from_cfg cfg.alba_blob_io_engine)
    in
    { cfg; abm;nsm; proxy ; maintenance; osds ; etcd }

  let to_arakoon_189 t =
    let new_binary = t.cfg.arakoon_189_bin in
    let new_plugin_path = t.cfg.alba_06_plugin_path in
    let t' = { t with
               cfg = { t.cfg with
                       arakoon_bin = new_binary;
                       alba_plugin_path = new_plugin_path;
                     };
             }
    in
    t'.abm # to_arakoon_189;
    t'.nsm # to_arakoon_189;
    t'

  let setup_osds t =
    Array.iter (fun asd ->
                asd # persist_config;
                asd # start
               ) t.osds

  let claim_osd t long_id =
    let cmd = [
        "claim-osd";
        "--long-id"; long_id;
        "--config" ; t.abm # config_url |> Url.canonical;
      ]
    in
    _alba_cmd_line cmd


  let claim_osds t long_ids =
    List.fold_left
      (fun acc long_id ->
       try
         let () = claim_osd t long_id in
         long_id :: acc
       with _ -> acc
      )
      [] long_ids


  let _extract_result json_s =
    let json = Yojson.Safe.from_string json_s in
    let basic = Yojson.Safe.to_basic json  in
    match basic with
    | `Assoc [
        ("success", `Bool true);
        ("result", `List result)] -> result
    | _ -> failwith "unexpected json format"


  let parse_harvest osds_json_s =
    let result = _extract_result osds_json_s in
    List.fold_left
       (fun acc x ->
         begin
           let fields = Yojson.Basic.Util.to_assoc x in
           let get_field x = List.assoc x fields in
           let long_id_field = get_field "long_id" in
           let long_id = Yojson.Basic.Util.to_string long_id_field in
           long_id :: acc
         end
       )
       [] result

  let parse_used_bytes json_s =
    let result = _extract_result json_s in
    List.fold_left (fun acc x ->
        begin
          let fields = Yojson.Basic.Util.to_assoc x in
          let get_field x = List.assoc x fields in
          let id_field = get_field "id" in
          let id = Yojson.Basic.Util.to_int id_field in
          let used_bytes =
            try
              let used_bytes_field =
                get_field "used" in
              Some (Yojson.Basic.Util.to_int used_bytes_field)
            with _ -> None
          in
          (id, used_bytes)::acc
        end
      ) [] result

  let harvest_available_osds t =
    let available_json_s =
      let cmd =
        [t.cfg.alba_bin;
         "list-available-osds";
         "--config"; t.abm # config_url |> Url.canonical ;
         "--to-json"
        ]
      in
      let cmd' = if t.cfg.tls then _alba_extend_tls cmd else cmd in
      cmd' |> Shell.cmd_with_capture
    in
    parse_harvest available_json_s

  let is_local_osd t long_id =
    let suffix = t.cfg.local_nodeid_prefix in
    suffix = Str.last_chars long_id (String.length suffix)

  let list_osds t =
    let cmd = [
        t.cfg.alba_bin; "list-osds";
        "--config"; t.abm # config_url |> Url.canonical;
        "--to-json"

      ]
    in
    let cmd' = if t.cfg.tls then _alba_extend_tls cmd else cmd in
    cmd'  |> Shell.cmd_with_capture

  let check_used_bytes t =
    let json = list_osds t in
    let used_bytes = parse_used_bytes json in
    List.iter
      (fun (osd_id, used_bytes) ->
        match used_bytes with
        | Some v ->
           Printf.printf "(%i,%i)\n%!" osd_id v;
           if v < 0 then
             failwith (Printf.sprintf "osd:%i has used_bytes:%i ?" osd_id v)
        | None -> Printf.printf "(%i) has no attribute 'used' ?\n%!" osd_id
      )
      used_bytes


  let claim_local_osds t n =
    let do_round() =
      let long_ids = harvest_available_osds t in
      let locals =
        List.filter
          (is_local_osd t)
          long_ids
      in
      let claimed = claim_osds t locals in
      List.length claimed
    in
    let rec loop j c =
      if j = n
      then ()
      else if c > 20
      then failwith "could not claim enough local osds after 20 attempts"
      else
        let n_claimed = do_round() in
        Unix.sleep 1;
        loop (j+n_claimed) (c+1)
    in
    loop 0 0

  let stop_osds t =
    Array.iter (fun asd -> asd # stop) t.osds


  let restart_osds t =
    stop_osds t ;
    Unix.sleep 3;
    Array.iter
      (fun asd -> asd # start)
      t.osds




  let list_namespaces t  =
    let r = [t.cfg.alba_bin; "list-namespaces";
             "--config"; t.abm # config_url|> Url.canonical ;
             "--to-json";
            ] |>
              (fun cmd ->
               if t.cfg.tls
               then _alba_extend_tls cmd
               else cmd)
            |> Shell.cmd_with_capture in
    let json = Yojson.Safe.from_string r in
    let basic = Yojson.Safe.to_basic json  in
    match basic with
    | `Assoc [
        ("success", `Bool true);
        ("result", `List result)] ->
       List.map
         (function
             | `Assoc
               [("id", `Int id); ("name", `String name);
                ("nsm_host_id", `String nsm_host); ("state", `String state);
                ("preset_name", `String preset_name)]
               -> (id,name, nsm_host, state, preset_name)
             | _ -> failwith "bad structure"
         )
         result
    | _ -> failwith "?"

  let install_monitoring t =
    let arakoons = ["pgrep";"-a";"arakoon"] |> Shell.cmd_with_capture in
    let albas    = ["pgrep";"-a";"alba"]    |> Shell.cmd_with_capture in
    let oc = open_out t.cfg.monitoring_file in
    output_string oc arakoons;
    output_string oc "\n";
    output_string oc albas;
    output_string oc "\n";
    close_out oc;
    let get_pids text =
      let lines = Str.split (Str.regexp "\n") text in
      List.map (fun line -> Scanf.sscanf line "%i " (fun x -> x)) lines
    in
    let arakoon_pids = get_pids arakoons in
    let alba_pids = get_pids albas in
    let pids = arakoon_pids @ alba_pids in
    let args = List.fold_left (fun acc pid -> "-p"::(string_of_int pid):: acc) ["1"] pids in
    "pidstat" :: "-hudr" :: args |> Shell.detach ~out:t.cfg.monitoring_file




  let setup t =
    let cfg = t.cfg in
    let _ = _arakoon_cmd_line ["--version"] in
    let _ = _alba_cmd_line ~ignore_tls:true ["version"] in
    if cfg.tls
    then
      begin
        "mkdir -p " ^ cfg.arakoon_path |> Shell.cmd;
        make_ca cfg;
        let my_client = "my_client" in
        let client_path = cfg.arakoon_path ^ "/" ^ my_client in
        "mkdir " ^ client_path |> Shell.cmd;
        make_cert client_path my_client
      end;
    let () =
      match t.etcd with
      | None -> ()
      | Some etcd ->
         etcd # persist_config;
         etcd # start;
    in

    t.abm # persist_config;
    t.abm # start ;

    t.nsm # persist_config;
    t.nsm # start ;

    let _ = t.abm # wait_for_master () in
    let _ = t.nsm # wait_for_master () in

    t.proxy # persist_config;
    t.proxy # start;


    t.maintenance # write_config_file;
    t.maintenance # start;

    nsm_host_register t;

    setup_osds t;
    claim_local_osds t t.cfg.n_osds;

    t.proxy # create_namespace "demo";
    install_monitoring t


  let kill t =
    let cfg = t.cfg in
    let () = match t.etcd with
      | None -> ()
      | Some etcd -> etcd # stop
    in
    let pkill x = (Printf.sprintf "pkill -e -9 %s" x) |> Shell.cmd ~ignore_rc:true in
    pkill "arakoon";
    pkill "alba";
    pkill "'java.*SimulatorRunner.*'";
    "fuser -k -f " ^ cfg.monitoring_file |> Shell.cmd ~ignore_rc:true ;
    t.abm # remove_dirs;
    "rm -rf " ^ cfg.alba_base_path |> Shell.cmd;
    "rm -rf " ^ cfg.arakoon_path   |> Shell.cmd;
    ()

  let proxy_pid t =
    let n =
      let cmd = String.concat " " (t.proxy # start_cmd) in
      let s = Printf.sprintf "pgrep -f '%s'" cmd in
      try Shell.cmd_with_capture [s]
      with exn ->
        Shell.cmd (Printf.sprintf "tail -n 1000 %s" (t.proxy # log_file));
        raise exn
    in
    Scanf.sscanf n " %i" (fun i -> i)

  let smoke_test t =
    let _  = proxy_pid t in
    check_used_bytes t;

    ()

end

module JUnit = struct
  type result =
    | Ok
    | Err of string
    | Fail of string
    [@@deriving show]

  type testcase = {
      classname:string;
      name: string;
      time: float;
      result : result;
    } [@@deriving show]

  let make_testcase classname name time result = {classname;name;time; result}
  type suite = { name:string; time:float; tests : testcase list}[@@deriving show]

  let make_suite name tests time = {name;tests;time}

  let summary suite =
    List.fold_left
      (fun (n_errors,n_failures,n) test ->
       match test.result with
       | Ok     -> (n_errors,     n_failures    , n+1)
       | Err _  -> (n_errors + 1, n_failures    , n+1)
       | Fail _ -> (n_errors,     n_failures +1 , n+1)
      ) (0,0,0) suite.tests

  let dump_xml suites fn =
    let dump_test oc test =
      let element =
        Printf.sprintf
          "      <testcase classname=%S name=%S time=\"%f\" >\n"
          test.classname test.name test.time
      in
      output_string oc element;
      let () = match test.result with
      | Ok -> ()
      | Err s  -> output_string oc (Printf.sprintf "        <error>%s</error>\n" s)
      | Fail s -> output_string oc (Printf.sprintf "        <failure>%s</failure>\n" s)
      in
      output_string oc "      </testcase>\n"
    in
    let dump_suite oc suite =
      let element =
        let errors,failures,size = summary suite in
        Printf.sprintf
          ("    <testsuite errors=\"%i\" failures=\"%i\" name=%S skipped=\"0\" "
          ^^ "tests=\"%i\" time=\"%f\" >\n")
          errors failures
          suite.name size
          suite.time
      in
      output_string oc element;
      List.iter (fun test -> dump_test oc test) suite.tests;
      output_string oc "    </testsuite>\n";
    in
    let oc = open_out fn in
    output_string oc "<?xml version=\"1.0\" ?>\n";
    output_string oc "  <testsuites >\n";
    List.iter (fun suite -> dump_suite oc suite) suites;
    output_string oc "  </testsuites>\n";
    close_out oc

  let dump suites =
    Printf.printf "%s\n" ([% show : suite list] suites)

  let rc suites = List.fold_left (fun acc s ->
                                 let e,f,_ = summary s in e + f + acc
                                ) 0 suites
end

module Test = struct
  open Deployment
  type backend_connection_manager = {
      backend_type : string;
      alba_connection_host : string ;
      alba_connection_port : string ;
      alba_connection_transport : string;
    } [@@ deriving yojson, show]

  type backend_cfg = {
      backend_connection_manager : backend_connection_manager;
    } [@@ deriving yojson, show]

  let backend_cfg_dir cfg = cfg.workspace ^ "/tmp/voldrv"
  let backend_cfg_file cfg = backend_cfg_dir cfg  ^ "/backend.json"

  let make_backend_cfg cfg ~host ~port ~transport =
      {
        backend_connection_manager = {
          backend_type = "ALBA";
          alba_connection_host = host;
          alba_connection_port = port;
          alba_connection_transport = transport; (* "RDMA" | "TCP" *)
        }
      }

  let _get_ip_transport cfg =
    match cfg.alba_rdma with
        | None -> "127.0.0.1","TCP"
        | Some rdma -> rdma,"RDMA"

  let backend_cfg_persister cfg =
    let backend_cfg =
      let host, transport = _get_ip_transport cfg
      and port = "10000" in
      make_backend_cfg cfg
                       ~host
                       ~port
                       ~transport
    in
    let oc = open_out (backend_cfg_file cfg) in
    let json = backend_cfg_to_yojson backend_cfg in
    Yojson.Safe.pretty_to_channel oc json;
    close_out oc

  let wrapper f t =
    let t = Deployment.make_default () in
    Deployment.kill t;
    Deployment.setup t;
    let r = f t in
    let () = Deployment.smoke_test t in
    r

  let no_wrapper f t = f t

  let cpp ?(xml=false) ?filter ?dump (t:Deployment.t) =
    let cfg = t.Deployment.cfg in
    let host, transport = _get_ip_transport cfg
    and port = "10000"
    in
    let cmd =
      ["cd";cfg.alba_home; "&&";
       "LD_LIBRARY_PATH=./cpp/lib";
       Printf.sprintf "ALBA_PROXY_IP=%s" host;
       Printf.sprintf "ALBA_PROXY_PORT=%s" port;
       Printf.sprintf "ALBA_PROXY_TRANSPORT=%s" transport;
       "./cpp/bin/unit_tests.out";
      ]
    in
    let cmd2 = if xml then cmd @ ["--gtest_output=xml:gtestresults.xml" ] else cmd in
    let cmd3 = match filter with
      | None -> cmd2
      | Some f -> cmd2 @ ["--gtest_filter=" ^ f]
    in
    cmd3 |> String.concat " " |> Shell.cmd_with_rc

  let stress ?(xml=false) ?filter ?dump (t:Deployment.t) =
    let t0 = Unix.gettimeofday() in
    let n = 3000 in
    let rec loop i =
      if i = n
      then ()
      else
        let name = Printf.sprintf "%08i" i in
        let () = t.Deployment.proxy # create_namespace name in
        loop (i+1)
    in
    let () = loop 0 in
    let namespaces = Deployment.list_namespaces t in
    let t1 = Unix.gettimeofday () in
    let d = t1 -. t0 in
    assert ((n+1) = List.length namespaces);
    let open JUnit in
    let time = d in
    let testcase = make_testcase "package.test" "testname" time JUnit.Ok in
    let suite    = make_suite "stress test suite" [testcase] time in
    let suites   = [suite] in
    let () =
      if xml
      then dump_xml suites "testresults.xml"
      else dump suites
    in
    rc suites


  let ocaml ?(xml=false) ?filter ?dump t =
    begin

      let cfg = t.Deployment.cfg in
      if cfg.tls
      then
        begin (* make cert for extra asd (test_discover_claimed) *)
          let asd_id = "test_discover_claimed" in
          let base = Printf.sprintf "%s/%s" cfg.arakoon_path asd_id in
          "mkdir -p " ^ base |> Shell.cmd;
          make_cert base asd_id;
        end;
      let cmd = [
          (*"valgrind"; "--track-origins=yes"; *)
          cfg.alba_bin; "unit-tests";
          "--config" ; t.abm # config_url |> Url.canonical
        ]
      in
      let cmd2 = if xml then cmd @ ["--xml=true"] else cmd in
      let cmd3 = if cfg.tls then _alba_extend_tls cmd2 else cmd2 in
      let cmd4 = match filter with
        | None -> cmd3
        | Some filter -> cmd3 @ ["--only-test=" ^ filter] in
      let cmd5 = match dump with
        | None -> cmd4
        | Some dump -> cmd4 @ [" > " ^ dump] in
      let cmd_s = cmd5 |> String.concat " " in
      cmd_s
      |> Shell.cmd_with_rc
    end

  let _make_backend_cfg_dir cfg =
    let cmd_s = ["mkdir" ; "-p" ; backend_cfg_dir cfg]
                |> String.concat " "
    in
    cmd_s |> Shell.cmd_with_rc |> ignore

  let voldrv_backend ?(xml=false) ?filter ?dump t =
    let cfg = t.Deployment.cfg in
    _make_backend_cfg_dir cfg;
    backend_cfg_persister cfg;
    let cmd = [
        cfg.voldrv_backend_test;
        "--skip-backend-setup"; "1";
        "--backend-config-file"; backend_cfg_file cfg;
        (*"--loglevel=error"; *)
      ]
    in
    let cmd2 = if xml then cmd @ ["--gtest_output=xml:gtestresults.xml"] else cmd in
    let cmd3 = match filter with
      | None -> cmd2
      | Some dump -> cmd2 @ []
    in
    let cmd4 = match dump with
      | None -> cmd3
      | Some dump -> cmd3 @ ["> " ^ dump ^ " 2>&1"]
    in

    let cmd_s = cmd4 |> String.concat " " in
    let () = Printf.printf "cmd_s = %s\n%!" cmd_s in
    cmd_s |> Shell.cmd_with_rc

  let voldrv_tests ?(xml = false) ?filter ?dump t =
    let cfg = t.Deployment.cfg in
    _make_backend_cfg_dir cfg;
    backend_cfg_persister cfg;
    let cmd = [cfg.voldrv_test;
               "--skip-backend-setup";"1";
               "--backend-config-file"; backend_cfg_file cfg;
               "--arakoon-binary-path"; cfg.arakoon_bin;
               "--loglevel=error"]
    in
    let cmd2 = if xml then cmd @ ["--gtest_output=xml:gtestresults.xml"] else cmd in
    let cmd3 = match filter with
      | None -> cmd2 @ ["--gtest_filter=SimpleVolumeTests/SimpleVolumeTest*"]
      | Some filter -> cmd2 @ ["--gtest_filter=" ^ filter]
    in
    let cmd4 = match dump with
      | None -> cmd3
      | Some dump -> cmd3 @ ["> " ^ dump ^ " 2>&1"]
    in
    let cmd_s = cmd4 |> String.concat " " in
    let () = Printf.printf "cmd_s = %s\n%!" cmd_s in
    cmd_s |> Shell.cmd_with_rc


  let disk_failures ?(xml= false) ?filter ?dump t =
    let cfg = t.Deployment.cfg in
    let cmd = [
        cfg.failure_tester;
        "--config" ; t.abm # config_url |> Url.canonical;
      ]
    in
    let cmd2 = if xml then cmd @ ["--xml=true"] else cmd in
    let cmd3 =
      if t.cfg.tls
      then _alba_extend_tls cmd2
      else cmd2
    in
    let cmd_s = cmd3 |> String.concat " " in
    let () = "free -h" |> Shell.cmd in
    let () = Printf.printf "cmd_s = %s\n%!" cmd_s in
    cmd_s |> Shell.cmd_with_rc

  let asd_start ?(xml=false) ?filter ?dump t =
    let cfg = t.Deployment.cfg in
    let t0 = Unix.gettimeofday() in
    let object_location = cfg.alba_base_path ^ "/obj" in
    let cmd_s = Printf.sprintf "dd if=/dev/urandom of=%s bs=1M count=1" object_location in
    cmd_s |> Shell.cmd;
    let rec loop i =
      if i = 1000
      then ()
      else
        let () = t.proxy # upload_object "demo" object_location (string_of_int i) in
        loop (i+1)
    in
    loop 0;
    Deployment.restart_osds t;

    let attempt ()  =
      try
        t.proxy # cmd_line
                ["proxy-upload-object";
                 "demo";
                 object_location;
                 "some_other_name";
                 "--allow-overwrite";
                ];
        true
      with
      | _ -> false
    in
    let () =
        attempt () |> ignore;
        attempt () |> ignore;
        attempt () |> ignore;
        Unix.sleep 2;
        attempt () |> ignore;
    in
    let ok = attempt () in
    Printf.printf "ok:%b\n%!" ok;
    Deployment.smoke_test t;
    let t1 = Unix.gettimeofday() in
    let d = t1 -. t0 in
    let open JUnit in
    let time = d in
    let testcase = make_testcase "package.test" "testname" time JUnit.Ok in
    let suite    = make_suite "stress test suite" [testcase] time in
    let suites   = [suite] in
    let () =
      if xml
      then dump_xml suites "testresults.xml"
      else dump suites
    in
    JUnit.rc suites

  let asd_version t =
    try
      let version_s = t.Deployment.osds.(1) # get_remote_version |> String.trim in
      Printf.printf "version_s=%S\n%!" version_s;
      match
        version_s.[0] = '(' &&
          String.length version_s > 4
      with
      | true -> JUnit.Ok
      | false ->JUnit.Fail "failed test"
    with exn -> JUnit.Err (Printexc.to_string exn)

  let _assert_parseable json =
    try
      let _ = Yojson.Safe.from_string json in
      JUnit.Ok
    with x -> JUnit.Err (Printexc.to_string x)

  let asd_statistics t =
    let stats_s = t.Deployment.osds.(1) # get_statistics in
    _assert_parseable stats_s

  let abm_statistics t =
    let cmd =
      [t.cfg.alba_bin;
       "mgr-statistics";
         "--config"; t.abm # config_url |> Url.canonical;
         (* TODO "--to-json"*)
        ]
    in
    let _stats_s =
      (if t.cfg.tls
       then _alba_extend_tls cmd
       else cmd)
      |> Shell.cmd_with_capture in
    (* TODO _assert_parseable stats_s *)
    JUnit.Ok

  let nsm_host_statistics t =
    let cmd =
      [t.cfg.alba_bin;
       "nsm-host-statistics";
       "--config";t.abm # config_url |> Url.canonical;
       t.nsm # cluster_id ;
       (* TODO "--to-json";*)
      ]
    in
    let _stats_s =
      (if t.cfg.tls
       then _alba_extend_tls cmd
       else cmd)
      |> Shell.cmd_with_capture in
    (* TODO _assert_parseable stats_s *)
    JUnit.Ok

  let asd_statistics_via_abm t =
    let long_id = t.Deployment.osds.(1) # long_id in

    let cmd= [
        t.cfg.alba_bin;
        "asd-statistics";
        "--config"; t.abm # config_url |> Url.canonical;
        "--long-id"; long_id;
        "--to-json";
      ]
    in
    let stats_s =
      (if t.cfg.tls
       then _alba_extend_tls cmd
       else cmd)
      |> Shell.cmd_with_capture in
    _assert_parseable stats_s

  let asd_crud t  =
    let k = "the_key"
    and v = "the_value" in
    let osd = t.Deployment.osds.(1) in
    osd # set k v;
    let v2 = osd # get k in
    match Str.search_forward (Str.regexp v) v2 0  <> -1 with
    | true      -> JUnit.Ok
    | false     -> JUnit.Fail (Printf.sprintf "%S <---> %S\n" v v2)
    | exception x -> JUnit.Err (Printexc.to_string x)

  let asd_cli_env t =
    let cfg = t.Deployment.cfg in
    if cfg.tls
    then
      let cert,pem,key = _get_client_tls () in
      let host,transport = _get_ip_transport cfg
      and port = "8501" in
      let cmd = [Printf.sprintf "ALBA_CLI_TLS='%s,%s,%s'" cert pem key;
                 t.cfg.alba_bin;
                 "asd-get-version";
                 "-h"; host;
                 "-p" ; port;
                 "-t" ; transport
                ]
      in
      let _r = Shell.cmd_with_capture cmd in
      JUnit.Ok
    else
      JUnit.Ok


  let create_example_preset t =
    let cmd = [
        "create-preset"; "example";
        "--config"; t.Deployment.abm # config_url |> Url.canonical;
        "< "; "./cfg/preset.json";
      ]
    in
    _alba_cmd_line ~cwd:t.cfg.alba_home cmd;
    JUnit.Ok

  let cli t =
    let suite_name = "run_tests_cli" in
    let tests = ["asd_crud", asd_crud;
                 "asd_version", asd_version;
                 "asd_statistics", asd_statistics;
                 "asd_statistics_via_abm", asd_statistics_via_abm;
                 "abm_statistics", abm_statistics;
                 "nsm_host_statistics", nsm_host_statistics;
                 "asd_cli_env", asd_cli_env;
                 "create_example_preset", create_example_preset;
                ]
    in
    let t0 = Unix.gettimeofday() in
    let results =
      List.fold_left (
          fun acc (name,test) ->

          let t0 = Unix.gettimeofday () in
          let result =
            try
              Printf.printf "Starting test %s\n" name;
              test t
            with x ->
              Printf.printf "Test %s failed: %s\n" name (Printexc.get_backtrace ());
              JUnit.Err (Printexc.to_string x)
          in
          let t1 = Unix.gettimeofday () in
          let d = t1 -. t0 in
          let testcase = JUnit.make_testcase name name d result in
          testcase ::acc
        ) [] tests
    in
    let t1 = Unix.gettimeofday() in
    let d = t1 -. t0 in
    let suite = JUnit.make_suite suite_name results d in
    suite

  let big_object t =
    let inner () =
      let preset = "preset_no_compression" in
      let namespace ="big" in
      let name = "big_object" in
      _alba_cmd_line ~cwd:t.Deployment.cfg.alba_home [
                       "create-preset"; preset;
                       "--config"; t.abm # config_url |> Url.canonical;
                       " < "; "./cfg/preset_no_compression.json";
                     ];
      _alba_cmd_line [
          "create-namespace";namespace ;preset;
          "--config"; t.abm # config_url |> Url.canonical;
        ];
      let cfg = t.cfg in
      let object_file = cfg.alba_base_path ^ "/obj" in
      "truncate -s 1G " ^ object_file |> Shell.cmd;
      t.proxy # upload_object   namespace object_file name;
      t.proxy # download_object namespace name (cfg.alba_base_path ^ "obj_download");
    in
    let test_name = "big_object" in
    let t0 = Unix.gettimeofday () in
    let result =
      try inner () ; JUnit.Ok
      with x -> JUnit.Err (Printexc.to_string x)
    in
    let t1 = Unix.gettimeofday () in
    let d = t1 -. t0 in
    let testcase = JUnit.make_testcase test_name test_name d result in
    let suite = JUnit.make_suite "big_object" [testcase] d in
    suite

  let arakoon_changes t =
    let inner () =
      let wait_for x =
        let rec loop j =
          if j = 0
          then ()
          else
            let () = Printf.printf "%i\n%!" j in
            let () = Unix.sleep 1 in
            loop (j-1)
        in
        loop x
      in
      Deployment.kill t;
      let two_nodes = new arakoon "abm" ["abm_0";"abm_1"] 4000 t.cfg.etcd in
      let t' = {t with abm = two_nodes } in

      let upload_albamgr_cfg cfg =
        _alba_cmd_line ["update-abm-client-config";"--attempts";"5";
                        "--config"; cfg]
      in
      let n_nodes_in_config () =
        let host,transport = _get_ip_transport t.cfg
        and port = "10000"
        in
        let r = [t'.cfg.alba_bin;
                 "proxy-client-cfg";
                 "-h"; host;
                 "-p"; port;
                 "-t"; transport;
                 " | grep port | wc"
                ] |> Shell.cmd_with_capture in
        let c = Scanf.sscanf r " %i " (fun i -> i) in
        c
      in
      Deployment.setup t';
      wait_for 10;
      two_nodes # stop;
      wait_for 1;

      print_endline "grow the cluster";
      let three_nodes = new arakoon "abm" ["abm_0";"abm_1";"abm_2"] 4000 t.cfg.etcd in
      three_nodes # persist_cluster_config ;
      three_nodes # link_plugins "abm_2";
      three_nodes # start_node "abm_1";
      three_nodes # start_node "abm_2";
      wait_for 20;
      three_nodes # start_node "abm_0";


      let maintenance_cfg = t'.maintenance # abm_config_url in

      (* update maintenance *)
      let maybe_copy cfg_url =
        match t.cfg.etcd with
        | None ->
           let f = cfg_url |> Url.as_file in
           let maintenance_cfg_f = maintenance_cfg |> Url.as_file in
           Shell.cp f maintenance_cfg_f
        | _ -> ()
      in
      maybe_copy (three_nodes # config_url);
      t'.maintenance # signal "USR1";
      wait_for 120;
      let c = n_nodes_in_config () in
      assert (c = 3);

      print_endline "shrink the cluster";
      three_nodes # stop;
      wait_for 1;
      two_nodes # persist_cluster_config;
      two_nodes # start;
      maybe_copy (t'.abm # config_url) ;
      upload_albamgr_cfg (two_nodes # config_url |> Url.canonical);
      wait_for 120;
      let c = n_nodes_in_config () in
      assert (c = 2);
      ()

    in
    let test_name = "arakoon_changes" in
    let t0 = Unix.gettimeofday () in
    let result =
      try inner () ; JUnit.Ok
      with x -> JUnit.Err (Printexc.to_string x)
    in
    let t1 = Unix.gettimeofday () in
    let d = t1 -. t0 in
    let testcase = JUnit.make_testcase test_name test_name d result in
    let suite = JUnit.make_suite "arakoon_changes" [testcase] d in
    suite

  let compat ?(xml=false) ?filter ?dump t =
    let test old_proxy old_plugins old_asd t =
      let cfg = t.cfg in
      try
        Deployment.smoke_test t;
        let make_cli ?(old=false) extra =
          let bin = if old then failwith "old bin?" else cfg.alba_bin in
          bin :: extra
        in
        let obj_name = "alba_binary"
        and ns = "demo"
        and host = "127.0.0.1"
        in
        let basic_tests =
          [
            ["proxy-upload-object"; "-h"; host; ns; cfg.alba_bin; obj_name];
            ["proxy-download-object"; "-h"; host; ns; obj_name; "/tmp/downloaded.bin"];
            ["delete-object"; ns; obj_name;
             "--config"; t.abm # config_url |> Url.canonical
            ];
          ]
        in
        List.iter
          (fun t -> t |> make_cli |> String.concat " " |> Shell.cmd )
          basic_tests;

        (* explicit backward compatible operations *)
        let r = make_cli ["list-all-osds";
                          "--config"; t.abm # config_url |> Url.canonical;
                          "--to-json"]
              |> Shell.cmd_with_capture
        in
        let osds = Deployment.parse_harvest r in
        let long_id = List.find (is_local_osd t) osds in

        (* decommission 1 asd *)
        make_cli ["decommission-osd";"--long-id"; long_id;
                  "--config"; t.abm # config_url |> Url.canonical]
        |> String.concat " "
        |> Shell.cmd;
        (* list them *)
        let decommissioning_s=
          make_cli ["list-decommissioning-osds";
                    "--config"; t.abm # config_url |> Url.canonical;
                    "--to-json"
                   ]
          |> Shell.cmd_with_capture
        in
        let decommissioning = decommissioning_s |> Deployment.parse_harvest in
        assert (List.length decommissioning = 1 )
    with exn ->
      Shell.cmd "pgrep -a alba";
      Shell.cmd "pgrep -a arakoon";
      raise exn
    in
    let deploy_and_test old_proxy old_plugins old_asds =
      let t =
        let maybe_old_asds tx =
          if old_asds
          then
            {tx with osds = make_osds tx.cfg.n_osds
                                      tx.cfg.local_nodeid_prefix
                                      tx.cfg.alba_base_path
                                      tx.cfg.arakoon_path
                                      tx.cfg.alba_06_bin
                                      ~etcd:tx.cfg.etcd
                                      false
                                      Pure
            }
          else tx
        in
        let maybe_old_plugins tx =
          if old_plugins
          then
            Deployment.to_arakoon_189 tx
          else tx
        in
        let maybe_old_proxy tx =
          if old_proxy
          then
            let old_proxy =
              new proxy 0 tx.cfg
                  tx.cfg.alba_06_bin
                  (tx.abm # config_url)
                  tx.cfg.etcd ~v06_proxy:true
                  10_000
            in
            {tx with proxy = old_proxy }
          else tx
        in
        Deployment.make_default ()
        |> maybe_old_asds
        |> maybe_old_plugins
        |> maybe_old_proxy
      in
      Deployment.kill t; (* too precise, previous deployment is slightly different  *)
      Shell.cmd "pkill alba" ~ignore_rc:true;
      Shell.cmd "pkill arakoon" ~ignore_rc:true;

      t.abm # persist_config;
      t.abm # start;

      t.nsm # persist_config;
      t.nsm # start;

      let _ = t.abm # wait_for_master () in
      let _ = t.nsm # wait_for_master () in

      if old_plugins
      then
        begin
          let signature = "3cd787f7a0bcb6c8dbf40a8b4a3a5f350fa87d1bff5b33f5d099ab850e44aaeca6e3206b595d7cb361eed28c5dd3c0f3b95531d931a31a058f3c054b04917797b7363457f7a156b5f36c9bf3e1a43b46e5c1e9ca3025c695ef366be6c36a1fc28f5648256a82ca392833a3050e1808e21ef3838d0c027cf6edaafedc8cfe2f2fc37bd95102b92e7de28042acc65b8b6af4cfb3a11dadce215986da3743f1be275200860d24446865c50cdae2ebe2d77c86f6d8b3907b20725cdb7489e0a1ba7e306c90ff0189c5299194598c44a537b0a460c2bf2569ab9bb99c72f6415a2f98c614d196d0538c8c19ef956d42094658dba8d59cfc4a024c18c1c677eb59299425ac2c225a559756dee125ef93c38c211cda69c892d26ca33b7bd2ca95f15bbc1bb755c46574432005b8afcab48a0a5ed489854cec24207cddc7ab632d8715c1fb4b1309b45376a49e4c2b4819f27d9d6c8170c59422a0b778b9c3ac18e677bc6fa6e2a2527365aca5d16d4bc6e22007debef1989d08adc9523be0a5d50309ef9393eace644260345bb3d442004c70097fffd29fe315127f6d19edd4f0f46ae2f10df4f162318c4174b1339286f8c07d5febdf24dc049a875347f6b2860ba3a71b82aba829f890192511d6eddaacb0c8be890799fb5cb353bce7366e8047c9a66b8ee07bf78af40b09b4b278d8af2a9333959213df6101c85dda61f2944237c8" in
          [t.cfg.alba_06_bin;
           "apply-license";
           t.cfg.license_file;
           signature;
           "--config"; t.abm # config_url |> Url.canonical
          ]
          |> String.concat " "
          |> Shell.cmd
        end;
      t.proxy # persist_config;
      t.proxy # start;

      t.maintenance # write_config_file;
      t.maintenance # start;

      Deployment.nsm_host_register t;
      Deployment.setup_osds t;
      Deployment.claim_local_osds t t.cfg.n_osds;
      t.proxy # create_namespace "demo";

      test old_proxy old_plugins old_asds t
    in
    let rec loop acc flavour =
      if flavour = 8
      then acc
      else
        let old_proxy   = flavour land 4 = 4
        and old_plugins = flavour land 2 = 2
        and old_asds    = flavour land 1 = 1
        and test_name   = Printf.sprintf "flavour_%i" flavour
        in
        let t0 = Unix.gettimeofday() in
        let result =
          try
            deploy_and_test old_proxy old_plugins old_asds;
            JUnit.Ok;
          with exn ->
            JUnit.Err (Printexc.to_string exn)
        in
        let t1 = Unix.gettimeofday() in
        let d = t1 -. t0 in
        let testcase = JUnit.make_testcase "TestCompat" test_name d result in
        loop (testcase :: acc) (flavour +1)
    in
    let t0 = Unix.gettimeofday () in
    let testcases = loop [] 0 in
    let t1 = Unix.gettimeofday () in
    let d = t1 -. t0 in
    let suite = JUnit.make_suite "compatibility" testcases d in
    let results = [suite] in
    let () =
      if xml
      then JUnit.dump_xml results "./testresults.xml"
      else JUnit.dump results
    in
    JUnit.rc results



  let everything_else ?(xml=false) ?filter ?dump t =
    let suites =
      [ big_object;
        cli;
        arakoon_changes;
      ]
    in
    let results = List.map (fun s -> s t) suites in
    let () =
      if xml
      then JUnit.dump_xml results "./testresults.xml"
      else JUnit.dump results
    in
    let (rc:int) = JUnit.rc results in
    rc


  let test_asd_no_blobs ?(xml=false) ?filter ?dump _t =
    let t = Deployment.make_default ~write_blobs:false () in
    Deployment.kill t;
    Deployment.setup t;

    let preset = "preset_no_checksums" in
    _alba_cmd_line ~cwd:t.Deployment.cfg.alba_home [
                     "create-preset"; preset;
                     "--config"; t.abm # config_url |> Url.canonical;
                     " < "; "./cfg/preset_no_checksums.json"; ];

    let ns = "test_asd_no_blobs" in
    _alba_cmd_line [
        "create-namespace"; ns ;preset;
        "--config"; t.abm # config_url |> Url.canonical;
      ];

    let objname = "x" in
    let object_location = t.cfg.alba_base_path ^ "/obj" in
    let cmd_s = Printf.sprintf "dd if=/dev/urandom of=%s bs=1M count=1" object_location in
    cmd_s |> Shell.cmd;
    t.proxy # upload_object ns object_location objname;
    t.proxy # download_object ns objname (t.cfg.alba_base_path ^ "/obj_download_dest");
    t.proxy # delete_object ns objname;

    t.proxy # delete_namespace ns;
    0


  let nil ?(xml=false) ?filter ?dump t =
    0

  let aaa ?xml ?filter ?dump _t =
    (* alba accelerated alba *)
    let workspace = env_or_default "WORKSPACE" (Unix.getcwd ()) in
    let cfg_ssd = Config.make ~workspace:(workspace ^ "/tmp/alba_ssd") () in
    let t_ssd = Deployment.make_default ~cfg:cfg_ssd ~base_port:4000 () in
    Deployment.kill t_ssd;
    Shell.cmd_with_capture [ "rm"; "-rf"; workspace ^ "/tmp" ] |> print_endline;
    Deployment.setup t_ssd;
    let cfg_hdd = Config.make ~workspace:(workspace ^ "/tmp/alba_hdd") () in
    let t_hdd =
      Deployment.make_default
        ~fragment_cache:Proxy_cfg.(Alba { albamgr_cfg_url = Url.canonical (t_ssd.abm # config_url);
                                          bucket_strategy = OneOnOne { prefix = "prefix";
                                                                       preset = "default"; };
                                          manifest_cache_size = 1_000_000;
                                          cache_on_read_ = true; cache_on_write_ = true;
                                        })
        ~cfg:cfg_hdd ~base_port:6000 ()
    in
    Deployment.setup t_hdd;

    let objname = "fdsij" in
    (* uploading is enough to trigger caching *)
    t_hdd.proxy # upload_object "demo" cfg_hdd.arakoon_bin objname;

    let output = t_ssd.proxy # list_namespaces in
    assert (output = "Found 2 namespaces: [\"demo\"; \"prefix\\000\\000\\000\\000\"]");
    0
end



let process_cmd_line () =
  let cmd_len = Array.length Sys.argv in
  Printf.printf "cmd_len:%i\n%!" cmd_len;
  let suites =
    [ "ocaml",           Test.ocaml, true;
      "cpp",             Test.cpp, true;
      "voldrv_backend",  Test.voldrv_backend, true;
      "voldrv_tests",    Test.voldrv_tests, true;
      "disk_failures",   Test.disk_failures, true;
      "stress",          Test.stress,true;
      "asd_start",       Test.asd_start,true;
      "everything_else", Test.everything_else, true;
      "compat",          Test.compat, false;
      "asd_no_blobs",    Test.test_asd_no_blobs, false;
      "nil",             Test.nil, true;
      "aaa",             Test.aaa, false;
    ]
  in
  let print_suites () =
    Printf.printf "Available suites:\n   %s\n"
                  (String.concat
                     "\n   "
                     (List.map (fun (cmd, _, _) -> cmd) suites))
  in
  if cmd_len = 2
  then
    let _, test, setup =
      try
        List.find
          (fun (cmd, _, _) -> Sys.argv.(1) = cmd)
          suites
      with Not_found ->
        Printf.printf "Could not find suite %s\n" Sys.argv.(1);
        print_suites ();
        exit 1
    in
    let t = Deployment.make_default () in
    let w =
      if setup
      then Test.wrapper
      else Test.no_wrapper
    in
    let rc = w (test ~xml:true) t in
    exit rc
  else
    begin
      print_suites ();
      exit 1
    end

let () =
  if !Sys.interactive
  then ()
  else process_cmd_line ()

let top_level_run test =
  let t = Deployment.make_default () in
  Test.wrapper test t
