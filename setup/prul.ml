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

let peers_s peers =
  String.concat "," (List.map (fun (h,p) -> Printf.sprintf "%s:%i" h p) peers)

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
  let _t0 = Unix.gettimeofday()

  let _print x =
    let colour = 2 in
    let t = Unix.gettimeofday() in
    let d = t -. _t0 in
    Printf.printf "\027[38;5;%dm[%03.1f\t%s] \027[0m%s\n%!" colour d "shell" x


  let cmd ?(ignore_rc=false) x =
    _print x;
    let rc = x |> Sys.command in
    match ignore_rc,rc with
    | true,_ | false,0 -> ()
    | false, rc -> failwith (Printf.sprintf "%S=x => rc=%i" x rc)


  let cmd_with_rc x =
    _print x;
    Sys.command x

  let cmd_with_capture cmd =
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
    | WEXITED rc ->
       if rc = 0 then result
       else failwith "bad_rc"
    | WSIGNALED signal -> failwith "signal?"
    | WSTOPPED x -> failwith "stopped?"

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
