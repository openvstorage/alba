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

let () = Random.self_init ()

let get_some = function
  | Some x -> x
  | None -> failwith "get_some"

let get_some_default default = function
  | Some x -> x
  | None   -> default

let map_option f = function
  | Some x -> Some (f x)
  | None   -> None

let peers_s peers =
  String.concat "," (List.map (fun (h,p) -> Printf.sprintf "%s:%i" h p) peers)

let local_ip_address () =
  let open Unix in
  let s = socket PF_INET SOCK_DGRAM 0 in
  let sa = ADDR_INET((inet_addr_of_string "8.8.8.8"),0) in
  let () = connect s sa in
  let sa2 = getsockname s in
  match sa2 with
  | ADDR_UNIX _     -> failwith "can't happen"
  | ADDR_INET (x,_) ->
     string_of_inet_addr x

module Url = struct
  type t =
    | File of string
    | Etcd of ((string * int) list * string)

  let canonical = function
    | File f -> (*Printf.sprintf "file://%s" f*) f
    | Etcd (peers,path) -> Printf.sprintf "etcd://%s%s" (peers_s peers) path

  let to_yojson t =
    `String (canonical t)

  let of_yojson = function
    | _ -> failwith "todo"
  let as_file = function
    | File f -> f
    | _ -> failwith "not supported"
end

module Shell = struct
  let _print x =
    let colour = 2 in
    let time_f = Unix.gettimeofday () in
    let time = Unix.localtime time_f in
    Printf.printf
      "\027[38;5;%dm[%s %02d:%02d:%02d.%06d] \027[0m%s\n%!"
      colour "shell"
      time.Unix.tm_hour time.Unix.tm_min time.Unix.tm_sec
      (int_of_float ((mod_float time_f 1.) *. 1_000_000.))
      x

  let cmd ?(ignore_rc=false) x =
    _print x;
    let rc = x |> Sys.command in
    match ignore_rc,rc with
    | true,_ | false,0 -> ()
    | false, rc -> failwith (Printf.sprintf "%S=x => rc=%i" x rc)

  let cmd' ?(ignore_rc=false) xs =
    xs
    |> String.concat " "
    |> cmd ~ignore_rc

  let cmd_with_rc x =
    _print x;
    Sys.command x

  let cmd_with_capture_and_rc cmd =
    let line = String.concat " " cmd in
    _print line;
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
    | WEXITED rc -> result, rc
    | WSIGNALED signal -> failwith "signal?"
    | WSTOPPED x -> failwith "stopped?"

  let cmd_with_capture cmd =
    let result,rc = cmd_with_capture_and_rc cmd in
    if rc = 0
    then result
    else Printf.sprintf "rc=%i" rc |> failwith

  let cat f = cmd_with_capture ["cat" ; f]

  let detach ?(out = "/dev/null") ?(pre=[]) inner =
    let x = pre @ [
        "nohup";
        String.concat " " inner;
        ">> " ^ out;
        "2>&1";
        "&"
      ]
    in
    String.concat " " x |> cmd

  let cp src tgt = Printf.sprintf "cp %s %s" src tgt |> cmd

  let mkdir p = "mkdir -p " ^ p |> cmd

  let md5sum file =
    let x = cmd_with_capture ["md5sum"; file] in
    Scanf.sscanf x "%s" (fun s -> s)

  let rm file =
    "rm " ^ file |> cmd
end

module Etcdctl = struct
  let path (peers,prefix) key = prefix ^ key

  let _cmd ((peers, prefix) as t) command key extra =
    let p_s = peers_s peers in
    let _key  = path t key in
    let cmd = [
        "etcdctl"; (* should be in path *)
        "--peers=" ^ p_s;
        command;
        _key;

      ] @ extra
    in
    let cmd_s = String.concat " " cmd in
    Shell.cmd cmd_s

  let set t key value =
    _cmd t "set" key [Printf.sprintf "'%s'" value;]

  let mkdir t dir =
    _cmd t "mkdir" dir []

  let url (peers,prefix) key =
    Url.Etcd (peers, prefix ^ key)
end

class type component =
  object
    method persist_config : unit
    method start : unit
    method stop  : unit
  end

let merge_result_xmls input output_file =
  let open Ezxmlm in
  let dtd = ref None in
  let (tests, failures, disabled, errors, timestamp, time, testsuite_members) =
    List.fold_left
      (fun (tests, failures, disabled, errors, timestamp, time, testsuite_members)
           (input_file, suite_prefix) ->
        let dtd', xml = from_channel (open_in input_file) in
        dtd := Some dtd';
        let testsuites_attrs, testsuites = member_with_attr "testsuites" xml in

        let testsuite_members =
          List.append
            testsuites
            testsuite_members
        in

        let timestamp = get_attr "timestamp" testsuites_attrs in
        let time = time +. (get_attr "time" testsuites_attrs |> float_of_string) in
        let tests = tests + (get_attr "tests" testsuites_attrs |> int_of_string) in
        let failures = failures + (get_attr "failures" testsuites_attrs |> int_of_string) in
        let disabled = disabled + (get_attr "disabled" testsuites_attrs |> int_of_string) in
        let errors = errors + (get_attr "errors" testsuites_attrs |> int_of_string) in
        tests, failures, disabled, errors, timestamp, time, testsuite_members)
      (0, 0, 0, 0, "", 0., [])
      input
  in
  let attrs = [ ("", "tests"), string_of_int tests;
                ("", "failures"), string_of_int failures;
                ("", "disabled"), string_of_int disabled;
                ("", "errors"), string_of_int errors;
                ("", "timestamp"), timestamp;
                ("", "time"), string_of_float time;
              ]
  in
  to_channel
    (open_out output_file)
    (match !dtd with
     | None -> assert false
     | Some x -> x)
    [ (make_tag
         "testsuites"
         (attrs, testsuite_members)); ]
