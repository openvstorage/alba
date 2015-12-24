let get_some = function
  | Some x -> x
  | None -> failwith "get_some"

module Config = struct
  let env_or_default x y =
    try
      Sys.getenv x
    with Not_found -> y

  let home = Sys.getenv "HOME"
  let workspace = env_or_default "WORKSPACE" ""

  let arakoon_home = env_or_default "ARAKOON_HOME" (home ^ "/workspace/ARAKOON/arakoon")

  let arakoon_bin = env_or_default "ARAKOON_BIN" (arakoon_home ^ "/arakoon.native")
  let arakoon_path = workspace ^ "/tmp/arakoon"

  let abm_nodes = ["abm_0";"abm_1";"abm_2"]

  let abm_path = arakoon_path ^ "/" ^ "abm"

  let alba_home = env_or_default "ALBA_HOME" workspace
  let alba_base_path = workspace ^ "/tmp/alba"

  let alba_bin  = env_or_default "ALBA_BIN" (alba_home  ^ "/ocaml/alba.native")
  let alba_plugin_path = env_or_default "ALBA_PLUGIN_HOME" (alba_home ^ "/ocaml")

  let failure_tester = alba_home ^ "/ocaml/disk_failure_tests.native"

  let monitoring_file = workspace ^ "/tmp/alba/monitor.txt"

  let local_nodeid_prefix = Printf.sprintf "%08x" (Random.bits ())
  let asd_path_t = env_or_default "ALBA_ASD_PATH_T" (alba_base_path ^ "/asd/%02i")

  let voldrv_test = env_or_default
                      "VOLDRV_TEST"
                      (home ^ "/workspace/VOLDRV/volume_driver_test")
  let voldrv_backend_test = env_or_default
                              "VOLDRV_BACKEND_TEST"
                              (home ^ "/workspace/VOLDRV/backend_test")
  let _N = 12

  let tls =
    let v = env_or_default "ALBA_TLS" "false" in
    Scanf.sscanf v "%b" (fun x -> x)

  let generate_serial =
    let serial = ref 0 in
    fun () ->
       let r = !serial in
       let () = incr serial in
       Printf.sprintf "%i" r


end

module Shell = struct
  let cmd ?(ignore_rc=false) x =
    Printf.printf "%s\n%!" x;
    let rc = x |> Sys.command in
    if not ignore_rc && rc <> 0
    then failwith (Printf.sprintf "%S=x => rc=%i" x rc)
    else ()

  let cmd_with_capture cmd =
    let line = String.concat " " cmd in
    Printf.printf "%s\n" line;
    let open Unix in
    let ic = open_process_in line in
    let read_line () =
      try
        Some (input_line ic)
      with End_of_file -> None
    in
    let rec loop acc =
      match read_line() with
      | None      -> String.concat "\n" (List.rev acc)
      | Some line -> loop (line :: acc)
    in
    let result = loop [] in
    let status = close_process_in ic in
    match status with
    | WEXITED rc ->
       if rc = 0 then result
       else failwith "bad_rc"
    | WSIGNALED signal -> failwith "signal?"
    | WSTOPPED x -> failwith "stopped?"

  let cat f = cmd_with_capture ["cat" ; f]

  let detach ?(out = "/dev/null") inner =
    let x = [
        "nohup";
        String.concat " " inner;
        ">> " ^ out;
        "2>&1";
        "&"
      ]
    in
    String.concat " " x |> cmd
end

let make_ca () =
  let cacert_req = Config.arakoon_path ^ "/cacert-req.pem" in
  Printf.printf "make %s\n%!" cacert_req ;
  let key = Config.arakoon_path ^ "/cacert.key" in

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
  let cacert = Config.arakoon_path ^ "/cacert.pem" in
  ["openssl";"x509";
   "-signkey"; key;
   "-req"; "-in"  ; cacert_req;
   "-out" ; cacert;
  ] |> String.concat " " |> Shell.cmd;

  "rm " ^ cacert_req |> Shell.cmd

let make_cert path name =
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
  let cacert = Config.arakoon_path ^ "/cacert.pem" in
  let name_pem = Printf.sprintf "%s/%s.pem" path name in
  ["openssl"; "x509"; "-req"; "-in" ; req;
   "-CA"; cacert;
   "-CAkey"; Config.arakoon_path ^ "/cacert.key";
   "-out"; name_pem;
   "-CAcreateserial";"-CAserial" ; Config.arakoon_path ^ "/cacert-serial.seq"
  ] |> String.concat " " |> Shell.cmd;

  "rm " ^ req |> Shell.cmd;

  (* verify *)
  ["openssl"; "verify";
   "-CAfile"; cacert;
   name_pem
  ] |> String.concat " " |> Shell.cmd


let _arakoon_cmd_line x = String.concat " " (Config.arakoon_bin :: x) |> Shell.cmd

let _get_client_tls () =
  let cacert = Config.arakoon_path ^ "/cacert.pem" in
  let pem    = Config.arakoon_path ^ "/my_client/my_client.pem" in
  let key    = Config.arakoon_path ^ "/my_client/my_client.key" in
  (cacert,pem,key)

class arakoon cluster_id nodes base_port tls =
  let cluster_path = Config.arakoon_path ^ "/" ^ cluster_id in
  let cfg_file = Config.arakoon_path ^ "/" ^ cluster_id ^ ".ini" in
  let _extend_tls cmd =
    let cacert,my_client_pem,my_client_key = _get_client_tls () in
    cmd @ [
        "-tls-ca-cert"; cacert;
        "-tls-cert"; my_client_pem;
        "-tls-key"; my_client_key;
      ]
  in
object (self)
  method config_file = cfg_file
  method write_node_config_files node =
    let dir_path = cluster_path ^ "/" ^ node in
    "mkdir -p " ^ dir_path |> Shell.cmd;
    Printf.sprintf
      "ln -fs %s/nsm_host_plugin.cmxs %s/nsm_host_plugin.cmxs"
      Config.alba_plugin_path dir_path |> Shell.cmd;
    Printf.sprintf
      "ln -fs %s/albamgr_plugin.cmxs %s/albamgr_plugin.cmxs"
      Config.alba_plugin_path dir_path |> Shell.cmd;
    if tls then make_cert dir_path node

  method write_cluster_config_file =
    let oc = open_out cfg_file in
    let w x = Printf.ksprintf (fun s -> output_string oc s) (x ^^ "\n") in
    w "[global]";
    w "cluster = %s" (String.concat ", " nodes);
    w "cluster_id = %s" cluster_id;
    w "plugins = albamgr_plugin nsm_host_plugin";
    w "";
    if tls
    then
      begin
        w "tls_ca_cert = %s/cacert.pem" Config.arakoon_path;
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
       let home = Config.arakoon_path ^ "/" ^ cluster_id ^ "/" ^ node in
       w "home = %s" home;
       w "log_level = debug";
       w "fsync = false";
       w "";
       if tls then
         begin
           w "tls_cert = %s/%s.pem" home node;
           w "tls_key =  %s/%s.key" home node;
           w "";
         end;

      )
      nodes;
    close_out oc

  method write_config_files =
    List.iter (self # write_node_config_files) nodes;
    self # write_cluster_config_file



  method start_node node =
    [Config.arakoon_bin;
        "--node"; node;
        "-config"; cfg_file
    ] |> Shell.detach

  method start =
    List.iter (self # start_node) nodes

  method stop_node name =
    let pid_line = ["pgrep -a arakoon"; "| grep "; name ] |> Shell.cmd_with_capture in
    let pid = Scanf.sscanf pid_line " %i " (fun i -> i) in
    Printf.sprintf "kill %i" pid |> Shell.cmd

  method stop_all =
    List.iter (self # stop_node) nodes

  method remove_dirs =
    List.iter
      (fun node ->
       let rm = Printf.sprintf "rm -rf %s/%s" cluster_path node in
       let _ = Shell.cmd rm in
       ()
      )
      nodes

  method wait_for_master ?(max=15) () : string =
    let line = [Config.arakoon_bin; "--who-master";"-config"; cfg_file]
    in
    let line' = if Config.tls
                then _extend_tls line
                else line
    in
    let step () =
      try
        let r = Shell.cmd_with_capture line' in
        Some r
      with _ -> None
    in
    let rec loop n =
      if n = 0
      then failwith "No_master"
      else
        match step () with
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

let make_tls_client tls =
  if tls
  then
    let ca_cert = Config.arakoon_path ^ "/cacert.pem" in
    let my_client_pem = Config.arakoon_path ^ "/my_client/my_client.pem" in
    let my_client_key = Config.arakoon_path ^ "/my_client/my_client.key" in
    Some { ca_cert; creds = (my_client_pem, my_client_key)}
  else None
type proxy_cfg =
  { port: int;
    albamgr_cfg_file : string;
    log_level : string;
    fragment_cache_dir : string;
    manifest_cache_size : int;
    fragment_cache_size : int;
    tls_client : tls_client option;
  } [@@deriving yojson]

let make_proxy_config id abm_cfg_file base tls_client=
  { port = 10000 + id;
    albamgr_cfg_file = abm_cfg_file;
    log_level = "debug";
    fragment_cache_dir  = base ^ "/fragment_cache";
    manifest_cache_size = 100 * 1000;
    fragment_cache_size = 100 * 1000 * 1000;
    tls_client;
  }

let _alba_extend_tls cmd =
  let cacert = Config.arakoon_path ^ "/cacert.pem" in
  let my_client_pem = Config.arakoon_path ^ "/my_client/my_client.pem" in
  let my_client_key = Config.arakoon_path ^ "/my_client/my_client.key" in
  cmd @ [Printf.sprintf
           "--tls=%s,%s,%s" cacert my_client_pem my_client_key]

let _alba_cmd_line ?cwd ?(ignore_tls=false) x =
  let maybe_extend_tls cmd =
    if not ignore_tls && Config.tls
    then
      begin
        _alba_extend_tls cmd
      end
    else cmd
  in
  let cmd = (Config.alba_bin :: x) in
  let cmd1 = match cwd with
    | Some dir -> "cd":: dir ::"&&":: cmd
    | None -> cmd
  in
  cmd1
  |> maybe_extend_tls
  |> String.concat " "
  |> Shell.cmd ~ignore_rc:false

class proxy id abm_cfg_file tls =
  let proxy_base = Printf.sprintf "%s/proxies/%02i" Config.alba_base_path id in
  let cfg_file = proxy_base ^ "/proxy.cfg" in
  let tls_client = make_tls_client tls in
  let cfg = make_proxy_config id abm_cfg_file proxy_base tls_client in
  object

  method write_config_file :unit =
    "mkdir -p " ^ proxy_base |> Shell.cmd;
    let oc = open_out cfg_file in
    let json = proxy_cfg_to_yojson cfg in
    Yojson.Safe.pretty_to_channel oc json ;
    close_out oc

  method start : unit =
    let out = Printf.sprintf "%s/proxy.out" proxy_base in
    "mkdir -p " ^ cfg.fragment_cache_dir |> Shell.cmd;
    [Config.alba_bin; "proxy-start"; "--config"; cfg_file]
    |> Shell.detach ~out

  method upload_object namespace file name =
    ["proxy-upload-object";
     "-h";"127.0.0.1";
     namespace; file ; name ]
    |> _alba_cmd_line ~ignore_tls:true
  method download_object namespace name file =
    ["proxy-download-object";
     "-h";"127.0.0.1";
     namespace; name ;file ]
    |> _alba_cmd_line ~ignore_tls:true
end

type maintenance_cfg = {
    albamgr_cfg_file : string;
    log_level : string;
    tls_client : tls_client option;
  } [@@deriving yojson]

let make_maintenance_config abm_cfg_file tls_client =
  { albamgr_cfg_file = abm_cfg_file;
    log_level = "debug";
    tls_client ;
  }

class maintenance id abm_cfg_file tls =
  let maintenance_base =
    Printf.sprintf "%s/maintenance/%02i" Config.alba_base_path id
  in
  let maintenance_abm_cfg_file = maintenance_base ^ "/abm.ini" in
  let tls_client = make_tls_client tls in
  let cfg = make_maintenance_config maintenance_abm_cfg_file tls_client in
  let cfg_file = maintenance_base ^ "/maintenance.cfg" in


  object
    method abm_config_file = maintenance_abm_cfg_file

    method write_config_file : unit =
      "mkdir -p " ^ maintenance_base |> Shell.cmd;
      let () =
        Printf.sprintf "cp %s %s" abm_cfg_file maintenance_abm_cfg_file |> Shell.cmd
      in
      let oc = open_out cfg_file in
      let json = maintenance_cfg_to_yojson cfg in
      Yojson.Safe.pretty_to_channel oc json;
      close_out oc

    method start =
      let out = Printf.sprintf "%s/maintenance.out" maintenance_base in
      [Config.alba_bin; "maintenance"; "--config"; cfg_file]
      |> Shell.detach ~out

    method signal s=
      let pid_line = ["pgrep -a alba"; "| grep 'maintenance' " ]
                     |> Shell.cmd_with_capture
      in
      let pid = Scanf.sscanf pid_line " %i " (fun i -> i) in
      Printf.sprintf "kill -s %s %i" s pid |> Shell.cmd


end



type tls = { cert:string; key:string; port : int} [@@ deriving yojson]

type asd_cfg = {
    node_id: string;
    home : string;
    log_level : string;
    port : int option;
    asd_id : string;
    limit : int;
    __sync_dont_use: bool;
    multicast: float option;
    tls: tls option;
  }[@@deriving yojson]

let make_asd_config node_id asd_id home port tls=
  {node_id;
   asd_id;
   home;
   port;
   log_level = "debug";
   limit= 99;
   __sync_dont_use = false;
   multicast = Some 10.0;
   tls;
  }

class asd node_id asd_id home port tls =
  let cfg = make_asd_config node_id asd_id home port tls in
  let cfg_file = home ^ "/cfg.json" in
  let kill_port = match port with
    | None ->
       begin
         match tls with
         | Some tls -> tls.port
         | None -> failwith "no port?"
       end
    | Some p -> p
  in
  object(self)
    method config_file = cfg_file

    method tls = tls

    method write_config_files =
      "mkdir -p " ^ home |> Shell.cmd;
      if Config.tls
      then
        begin
        let base = Printf.sprintf "%s/%s" Config.arakoon_path asd_id in
        "mkdir -p " ^ base |> Shell.cmd;
        make_cert base asd_id;
        end;
      let oc = open_out cfg_file in
      let json = asd_cfg_to_yojson cfg in
      Yojson.Safe.pretty_to_channel oc json ;
      close_out oc

    method start =
      let out = home ^ "/stdout" in
      [Config.alba_bin; "asd-start"; "--config"; cfg_file]
      |> Shell.detach ~out;

    method stop =
      Printf.sprintf "fuser -k -n tcp %i" kill_port
      |> Shell.cmd

    method private build_remote_cli ?(json=true) what  =
      let p = match tls with
        | Some tls -> tls.port
        | None -> begin match port with | Some p -> p | None -> failwith "bad config" end
      in
      let cmd0 = [ Config.alba_bin;]
                 @ what
                 @ ["-h"; "127.0.0.1";"-p"; string_of_int p;]
      in
      let cmd1 = if Config.tls then _alba_extend_tls cmd0 else cmd0 in
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





module Demo = struct

  let abm =
    let id = "abm"
    and nodes = ["abm_0"; "abm_1"; "abm_2"]
    and base_port = 4000 in
    new arakoon id nodes base_port Config.tls

  let nsm =
    let id = "nsm"
    and nodes = ["nsm_0";"nsm_1"; "nsm_2"]
    and base_port = 4100 in
    new arakoon id nodes base_port Config.tls


  let proxy = new proxy 0 (abm # config_file) Config.tls
  let maintenance = new maintenance 0 (abm # config_file) Config.tls

  let nsm_host_register ~(nsm:arakoon) : unit =
    let cfg_file = nsm # config_file in
    let cmd = ["add-nsm-host"; cfg_file ;
               "--config" ; abm # config_file ]
    in
    _alba_cmd_line cmd

  let osds =
    let base_port = 8000 in
    let rec loop asds j =
      if j = Config._N
      then List.rev asds |> Array.of_list
      else
        begin
          let port = base_port + j in
          let node_id = j lsr 2 in
          let node_id_s = Printf.sprintf "%s_%i" Config.local_nodeid_prefix node_id in
          let asd_id = Printf.sprintf "%04i_%02i_%s" port node_id Config.local_nodeid_prefix in
          let home = Config.alba_base_path ^ (Printf.sprintf "/asd/%02i" j) in
          let tls =
            if Config.tls
            then
              begin
                let port = port + 500 in
                let base = Printf.sprintf "%s/%s" Config.arakoon_path asd_id in
                Some { cert = Printf.sprintf "%s/%s.pem" base asd_id ;
                       key  = Printf.sprintf "%s/%s.key" base asd_id ;
                       port ;
                     }
              end
            else None
          in
          let asd = new asd node_id_s asd_id home (Some port) tls in
          loop (asd :: asds) (j+1)
        end
    in
    loop [] 0

  let setup_osds n =
    Array.iter (fun asd ->
                asd # write_config_files;
                asd # start
               ) osds

  let claim_osd long_id =
    let cmd = [
        "claim-osd";
        "--long-id"; long_id;
        "--config" ; abm # config_file;
      ]
    in
    _alba_cmd_line cmd


  let claim_osds long_ids =
    List.fold_left
      (fun acc long_id ->
       try let () = claim_osd long_id in long_id :: acc
       with _ -> acc
      )
      [] long_ids


  let harvest_available_osds () =
    let available_json_s =
      let cmd =
        [Config.alba_bin;
         "list-available-osds"; "--config"; abm # config_file ; "--to-json"
        ]
      in
      let cmd' = if Config.tls then _alba_extend_tls cmd else cmd in
      cmd' |> Shell.cmd_with_capture
    in
    let json = Yojson.Safe.from_string available_json_s in
    (*let () = Printf.printf "available_json:%S" available_json_s in*)
    let basic = Yojson.Safe.to_basic json  in
    match basic with
    | `Assoc [
        ("success", `Bool true);
        ("result", `List result)] ->
       begin
         (List.fold_left
            (fun acc x ->
             match x with
             | `Assoc (_::_
                       :: _ (* ips *)
                       :: _ (*("port",`Int port)*)
                       ::_ :: _
                       :: _ (*("node_id", `String node_id) *)
                       :: ("long_id", `String long_id)
                       :: _
                       :: _) ->
                long_id :: acc
             | _ -> acc
            ) [] result)
       end
    | _ -> failwith "?"

  let claim_local_osds n =
    let do_round() =
      let long_ids = harvest_available_osds () in
      let locals = List.filter (fun x -> true) long_ids in
      let claimed = claim_osds locals in
      List.length claimed
    in
    let rec loop j c =
      if j = n || c > 20
      then ()
      else
        let n_claimed = do_round() in
        Unix.sleep 1;
        loop (j+n_claimed) (c+1)
    in
    loop 0 0

  let stop_osds () =
    Array.iter (fun asd -> asd # stop) osds


  let restart_osds () =
    stop_osds ();
    Array.iter (fun asd -> asd # start) osds


  let proxy_create_namespace name =
    _alba_cmd_line ~ignore_tls:true ["proxy-create-namespace"; "-h"; "127.0.0.1"; name]

  let list_namespaces () =
    let r = [Config.alba_bin; "list-namespaces";
             "--config"; abm # config_file;
             "--to-json";
            ] |> Shell.cmd_with_capture in
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

  let install_monitoring () =
    let arakoons = ["pgrep";"-a";"arakoon"] |> Shell.cmd_with_capture in
    let albas    = ["pgrep";"-a";"alba"]    |> Shell.cmd_with_capture in
    let oc = open_out Config.monitoring_file in
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
    "pidstat" :: args |> Shell.detach ~out:Config.monitoring_file




  let setup ?(abm=abm) () =
    let _ = _arakoon_cmd_line ["--version"] in
    let _ = _alba_cmd_line ~ignore_tls:true ["version"] in
    if Config.tls
    then
      begin
        "mkdir -p " ^ Config.arakoon_path |> Shell.cmd;
        make_ca ();
        let my_client = "my_client" in
        let client_path = Config.arakoon_path ^ "/" ^ my_client in
        "mkdir " ^ client_path |> Shell.cmd;
        make_cert client_path my_client
      end;

    abm # write_config_files;
    abm # start ;

    nsm # write_config_files;
    nsm # start ;

    let _ = abm # wait_for_master () in
    let _ = nsm # wait_for_master () in

    proxy # write_config_file;
    proxy # start;


    maintenance # write_config_file;
    maintenance # start;

    nsm_host_register nsm;

    setup_osds Config._N;

    claim_local_osds Config._N;

    proxy_create_namespace "demo";
    install_monitoring ()


  let kill () =
    let pkill x = (Printf.sprintf "pkill -e -9 %s" x) |> Shell.cmd ~ignore_rc:true in
    pkill (Filename.basename Config.arakoon_bin);
    pkill (Filename.basename Config.alba_bin);
    pkill "'java.*SimulatorRunner.*'";
    "fuser -k -f " ^ Config.monitoring_file |> Shell.cmd ~ignore_rc:true ;
    abm # remove_dirs;
    "rm -rf " ^ Config.alba_base_path |> Shell.cmd;
    "rm -rf " ^ Config.arakoon_path |> Shell.cmd;
    ()

  let proxy_pid () =
    let n = ["fuser";"-n";"tcp";"10000"] |> Shell.cmd_with_capture in
    Scanf.sscanf n " %i" (fun i -> i)

  let smoke_test () =
    let _  = proxy_pid () in
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

  let dump_xml suites fn =
    let dump_test oc test =
      let element =
        Printf.sprintf
          "      <testcase classname=%S name=%S time=\"%f\" >\n"
          test.classname test.name test.time
      in
      let () = match test.result with
      | Ok -> ()
      | Err s  -> output_string oc (Printf.sprintf "        <error>%s</error>\n" s)
      | Fail s -> output_string oc (Printf.sprintf "        <failure>%s</failure" s)
      in
      output_string oc element;
      output_string oc "      </testcase>\n"
    in
    let dump_suite oc suite =
      let element =
        let errors,failures,size =
          List.fold_left
            (fun (n_errors,n_failures,n) test ->
             match test.result with
             | Ok     -> (n_errors,     n_failures    , n+1)
             | Err _  -> (n_errors + 1, n_failures    , n+1)
             | Fail _ -> (n_errors,     n_failures +1 , n+1)
            ) (0,0,0) suite.tests
        in
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
end
module Test = struct

  let wrapper f =
    Demo.kill ();
    Demo.setup ();
    f ();
    Demo.smoke_test ()

  let cpp ?(xml=false) ?filter ?dump () =
    let cmd =
      ["cd";Config.alba_home; "&&"; "LD_LIBRARY_PATH=./cpp/lib"; "./cpp/bin/unit_tests.out";
      ]
    in
    let cmd2 = if xml then cmd @ ["--gtest_output=xml:gtestresults.xml" ] else cmd in
    let cmd3 = match filter with
      | None -> cmd2
      | Some f -> cmd2 @ ["--gtest_filter=" ^ f]
    in
    cmd3 |> String.concat " " |> Shell.cmd

  let stress ?(xml=false) ?filter ?dump () =
    let t0 = Unix.gettimeofday() in
    let n = 3000 in
    let rec loop i =
      if i = n
      then ()
      else
        let name = Printf.sprintf "%08i" i in
        let () = Demo.proxy_create_namespace name in
        loop (i+1)
    in
    let () = loop 0 in
    let namespaces = Demo.list_namespaces () in
    let t1 = Unix.gettimeofday () in
    let d = t1 -. t0 in
    assert ((n+1) = List.length namespaces);
    if xml
    then
      begin
        let open JUnit in
        let time = d in
        let testcase = make_testcase "package.test" "testname" time JUnit.Ok in
        let suite    = make_suite "stress test suite" [testcase] time in
        let suites   = [suite] in
        dump_xml suites "testresults.xml"
      end
    else ()


  let ocaml ?(xml=false) ?filter ?dump () =
    begin
      (* make cert for extra asd (test_discover_claimed) *)
      if Config.tls
      then
        begin failwith "todo"
        end;
      let cmd = [Config.alba_bin; "unit-tests"; "--config" ; Demo.abm # config_file ] in
      let cmd2 = if xml then cmd @ ["--xml=true"] else cmd in
      let cmd3 = match filter with
        | None -> cmd2
        | Some filter -> cmd2 @ ["--only-test=" ^ filter] in
      let cmd4 = match dump with
        | None -> cmd3
        | Some dump -> cmd3 @ [" > " ^ dump] in
      let cmd_s = cmd4 |> String.concat " " in
      let () = Printf.printf "cmd_s = %s\n%!" cmd_s in
      cmd_s
      |> Shell.cmd
    end

  let voldrv_backend ?(xml=false) ?filter ?dump ()=
    let cmd = [
        Config.voldrv_backend_test;
        "--skip-backend-setup"; "1";
        "--backend-config-file"; Config.alba_home ^ "/cfg/backend.json";
        "--loglevel=error";
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
    cmd_s |> Shell.cmd

  let voldrv_tests ?(xml = false) ?filter ?dump () =
    let cmd = [Config.voldrv_test;
               "--skip-backend-setup";"1";
               "--backend-config-file"; Config.alba_home ^ "/cfg/backend.json";
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
    cmd_s |> Shell.cmd


  let disk_failures ?(xml= false) ?filter ?dump () =
    let cmd = [
        Config.failure_tester;
        "--config" ; Demo.abm # config_file;
      ]
    in
    let cmd2 = if xml then cmd @ ["--xml=true"] else cmd in
    let cmd_s = cmd2 |> String.concat " " in
    let () = Printf.printf "cmd_s = %s\n%!" cmd_s in
    cmd_s |> Shell.cmd

  let asd_start ?(xml=false) ?filter ?dump () =
    let t0 = Unix.gettimeofday() in
    let object_location = Config.alba_base_path ^ "/obj" in
    let cmd_s = Printf.sprintf "dd if=/dev/urandom of=%s bs=1M count=1" object_location in
    cmd_s |> Shell.cmd;
    let rec loop i =
      if i = 1000
      then ()
      else
        let () = Demo.proxy # upload_object "demo" object_location (string_of_int i) in
        loop (i+1)
    in
    loop 0;
    Demo.restart_osds ();
    let attempt ()  =
      try [
        "proxy-upload-object";
        "-h";"127.0.0.1";
        "demo";object_location;
        "some_other_name";"--allow-overwrite";
        ] |> _alba_cmd_line ~ignore_tls:true;
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
    Demo.smoke_test();
    let t1 = Unix.gettimeofday() in
    let d = t1 -. t0 in
    if xml
    then
      begin
        let open JUnit in
        let time = d in
        let testcase = make_testcase "package.test" "testname" time JUnit.Ok in
        let suite    = make_suite "stress test suite" [testcase] time in
        let suites   = [suite] in
        dump_xml suites "testresults.xml"
      end
    else
      ()

  let asd_get_version() =
    try
      let version_s = Demo.osds.(1) # get_remote_version in
      match
        version_s.[0] = '(' &&
          String.length version_s > 4
      with
      | true -> JUnit.Ok
      | false ->JUnit.Fail "failed test"
    with exn -> JUnit.Err (Printexc.to_string exn)

  let asd_get_statistics () =
    let stats_s = Demo.osds.(1) # get_statistics in
    try
      let _ = Yojson.Safe.from_string stats_s in
      JUnit.Ok
    with x -> JUnit.Err (Printexc.to_string x)

  let asd_crud () =
    let k = "the_key"
    and v = "the_value" in
    let osd = Demo.osds.(1) in
    osd # set k v;
    let v2 = osd # get k in
    match Str.search_forward (Str.regexp v) v2 0  <> -1 with
    | true      -> JUnit.Ok
    | false     -> JUnit.Fail (Printf.sprintf "%S <---> %S\n" v v2)
    | exception x -> JUnit.Err (Printexc.to_string x)

  let asd_cli_env () =
    if Config.tls
    then
      let cert,pem,key = _get_client_tls () in
      let cmd = [Printf.sprintf "'%s,%s,%s'" cert pem key;
                 "&&" ;
                 "asd-get-version";
                 "-p" ; "8501"
                ]
      in
      let _r = Shell.cmd_with_capture cmd in
      JUnit.Ok
    else
      JUnit.Ok


  let create_example_preset () =
    let cmd = [
        "create-preset"; "example";
        "--config"; Demo.abm # config_file;
        "< "; "./cfg/preset.json";
      ]
    in
    try
      _alba_cmd_line ~cwd:Config.alba_home cmd;
      JUnit.Ok
    with | x -> JUnit.Err (Printexc.to_string x)

  let cli () =
    let suite_name = "run_tests_cli" in
    let tests = ["asd_crud", asd_crud;
                 "asd_get_version", asd_get_version;
                 "asd_get_statistics", asd_get_statistics;
                 "asd_cli_env", asd_cli_env;
                 "create_example_preset", create_example_preset;
                ]
    in
    let t0 = Unix.gettimeofday() in
    let results =
      List.fold_left (
          fun acc (name,test) ->

          let t0 = Unix.gettimeofday () in
          let result = test () in
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

  let big_object () =
    let inner () =
      let preset = "preset_no_compression" in
      let namespace ="big" in
      let name = "big_object" in
      _alba_cmd_line ~cwd:Config.alba_home [
                       "create-preset"; preset;
                       "--config"; Demo.abm # config_file;
                       " < "; "./cfg/preset_no_compression.json";
                     ];
      _alba_cmd_line [
          "create-namespace";namespace ;preset;
          "--config"; Demo.abm # config_file;
        ];
      let object_file = Config.alba_base_path ^ "/obj" in
      "truncate -s 2G " ^ object_file |> Shell.cmd;
      Demo.proxy # upload_object   namespace object_file name;
      Demo.proxy # download_object namespace name (Config.alba_base_path ^ "obj_download");
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

  let arakoon_changes () =
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
      Demo.kill ();
      let two_nodes = new arakoon "abm" ["abm_0";"abm_1"] 4000 Config.tls in
      Demo.setup ~abm:two_nodes ();
      wait_for 10;
      two_nodes # stop_node "abm_0";
      two_nodes # stop_node "abm_1";

      (* restart with other config *)
      let three_nodes = new arakoon "abm" ["abm_0";"abm_1";"abm_2"] 4000 Config.tls in
      three_nodes # write_cluster_config_file ;
      three_nodes # write_node_config_files "abm_2";
      three_nodes # start_node "abm_1";
      three_nodes # start_node "abm_2";
      wait_for 20;
      three_nodes # start_node "abm_0";

      (* update maintenance *)
      Printf.sprintf
        "cp %s %s"
        (three_nodes # config_file)
        (Demo.maintenance # abm_config_file) |> Shell.cmd;

      Demo.maintenance # signal "USR1";
      wait_for(120);
      let r = [Config.alba_bin; "proxy-client-cfg | grep port | wc" ] |> Shell.cmd_with_capture in
      let c = Scanf.sscanf r " %i " (fun i -> i) in
      assert (c = 3);
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




  let everything_else ?(xml=false) ?filter ?dump() =
    let suites =
      [big_object;
       cli;
       arakoon_changes;
      ]
    in
    let results = List.map (fun s -> s() ) suites in
    if xml
    then
       JUnit.dump_xml results "./testresults.xml"
    else
      JUnit.dump results


end


let () =
  let cmd_len = Array.length Sys.argv in
  Printf.printf "cmd_len:%i\n%!" cmd_len;
  if cmd_len = 2
  then
    let test = match Sys.argv.(1) with
      | "ocaml"           -> Test.ocaml
      | "cpp"             -> Test.cpp
      | "voldrv_backend"  -> Test.voldrv_backend
      | "voldrv_tests"    -> Test.voldrv_tests
      | "disk_failures"   -> Test.disk_failures
      | "stress"          -> Test.stress
      | "asd_start"       -> Test.asd_start
      | "everything_else" -> Test.everything_else
      | _  -> failwith "no test"
    in
    let f () = test ~xml:true () in Test.wrapper f