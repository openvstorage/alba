open Ocamlbuild_plugin

let dependencies =
  List.sort
    String.compare
    [
      (*"str";(* distributed with OCaml, so we know the version ... *)*)
      "lwt";
      "lwt.unix";
      "ssl";
      "ocplib-endian";
      "oUnit";
      "arakoon_client";
      "ppx_deriving.show";
      "ppx_deriving.enum";
      "ppx_deriving_yojson";
      "ctypes";
      "ctypes.foreign";
      "cstruct";
      "sexplib";
      "rocks";
      "cmdliner";
      "snappy";
      "bz2";
      "tiny_json";
      "yojson";
      "kinetic-client";
      "core";
      "redis";
      "uri";
  ]

let run_cmd cmd () =
  try
    let ch = Unix.open_process_in cmd in
    let line = input_line ch in
    let () = close_in ch in
    line
  with | End_of_file -> "Not available"

let time () =
  let open Unix in
  let tm = gmtime (time()) in
  Printf.sprintf "%02d/%02d/%04d %02d:%02d:%02d UTC"
                 (tm.tm_mday) (tm.tm_mon + 1) (tm.tm_year + 1900)
                 tm.tm_hour tm.tm_min tm.tm_sec



let list_dependencies () =
  let query pkg =
    let package = Findlib.query pkg in
    (pkg, package.Findlib.version, package.Findlib.description)
  in
  let pkgs = List.map query dependencies in
  let lines =
    List.map
      (fun (pkg, version, descr) ->
       Printf.sprintf "%-20s\t%16s\t%s"
                      pkg version descr
      )
      pkgs
  in
  String.concat "\n" lines


let major_minor_patch () =
  let tag_version = run_cmd "git describe --tags --exact-match --dirty"
  and branch_version = run_cmd "git describe --all" in
  let (major, minor, patch) =
    try
      Scanf.sscanf (tag_version ()) "%i.%i.%i" (fun ma mi p -> (ma,mi,p))
    with _ ->
      let bv = branch_version () in
      try
        Scanf.sscanf bv "heads/%i.%i" (fun ma mi -> (ma,mi,-1))
      with _ ->
        (* This one matches what's on Jenkins slaves *)
        try
          Scanf.sscanf bv "remotes/origin/%i.%i" (fun ma mi -> (ma, mi, -1))
        with _ -> (-1,-1,-1)
  in
  Printf.sprintf "(%d, %d, %d)" major minor patch

let make_version _ _ =
  let stringify v = Printf.sprintf "%S" v
  and id = fun x -> x in
  let fields = [ "git_revision", run_cmd "git describe --all --long --always --dirty", stringify
               ; "compile_time", time, stringify
               ; "git_repo",run_cmd "git config --get remote.origin.url", stringify
               ; "machine", run_cmd "uname -mnrpio", stringify
               ; "compiler_version", run_cmd "ocamlopt -version", stringify
               ; "(major, minor, patch)", major_minor_patch, id
               ; "dependencies", list_dependencies, stringify
               ]
  in
  let vals = List.map (fun (n, f, r) -> (n, f (), r)) fields in
  let lines = List.map (fun (n, v, r) -> Printf.sprintf "let %s = %s\n" n (r v)) vals in
  let lines' = lines @ ["let summary = (major,minor,patch, git_revision)"] in
  Echo (lines', "alba_version.ml")

let _ = dispatch &
          function
          | After_rules ->
             rule "alba_version.ml" ~prod:"alba_version.ml" make_version;

             flag["ocaml";"compile"]
                 (S[
                    A"-w";A "+1";
                    A"-w";A "+2";
                    A"-w";A "+3";
                    (*A"-w";A "+4";*)
                    A"-w";A "+5";
                    A"-w";A "+6";
                    A"-w";A "+7";
                    A"-w";A "+8";
                    (*A"-w";A "+9";*)

                    A"-w";A "+10";
                    A"-w";A "+11";
                    A"-w";A "+12";
                    A"-w";A "+13";

                    A"-w";A "+X"; (* 14..30 *)
                    A"-w";A "+K"; A"-w";A "-39";
                    A"-w";A "+40";
                    (*A"-w";A "+41";*)
                    (* A"-w";A "+42"; *)
                    A"-w";A "+43";
                    (*A"-w";A "+44";*)
                    (*A"-w";A "+45";*)
                   ]);
             dep ["ocaml";"link"]
                 ["src/tools/alba_crc32c_stubs.o";
                  "src/tools/alba_gcrypt_stubs.o";
                  "src/tools/alba_wrappers_stubs.o";
                  "src/other/posix_stubs.o"
                 ];

             flag ["c";"compile"]
                  (S[(*A"-ccopt"; A"-march=opteron"; *)
                     A"-ccopt"; A"-Wall";
                     A"-ccopt"; A"-Wextra";
                     A"-ccopt"; A"-Werror";
                     A"-ccopt"; A"-ggdb3";
                     A"-ccopt"; A"-O2";
                  ]);
             flag ["ocaml"; "compile"; "ppx_lwt"] &
               (*S [A "-ppx"; A "ppx_lwt -log -no-debug";];*)
               S [A "-ppx"; A "ppx_lwt -log";];

             flag ["link";"ocaml";"use_rocks"]
                  (S[A"-cclib";A"-lrocksdb"]);
             flag ["link";"ocaml";"use_gcrypt"]
                  (S[A"-cclib";A"-lgcrypt"]);
             flag ["link";"ocaml";"use_Jerasure"]
                  (S[A"-cclib";A"-lJerasure"]);
             flag ["link";"ocaml";"use_isal"]
                  (S[A"-cclib";A"-lisal"]);

          | _ -> ()
