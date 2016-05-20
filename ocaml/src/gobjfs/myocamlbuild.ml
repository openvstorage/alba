open Ocamlbuild_plugin
open Unix

let run_cmd cmd () =
  try
    let ch = Unix.open_process_in cmd in
    let line = input_line ch in
    let () = close_in ch in
    line
  with | End_of_file -> "Not available"


let make_version_and_meta _ _ =
  let tag_version    = run_cmd "git describe --tags --exact-match --dirty" in
  let branch_version = run_cmd "git describe --all" in
  let (major,minor,patch) =
    try
      Scanf.sscanf (tag_version ()) "%i.%i.%i" (fun ma mi p -> (ma,mi,p))
    with _ ->
      let bv = branch_version () in
      try Scanf.sscanf bv "heads/%i.%i" (fun ma mi -> (ma,mi,-1))
      with _ -> (-1,-1,-1)
  in
  let git_revision = run_cmd "git describe --all --long --always --dirty" () in
  let lines = [
      Printf.sprintf "let major = %i\n" major;
      Printf.sprintf "let minor = %i\n" minor;
      Printf.sprintf "let patch = %i\n" patch;
      Printf.sprintf "let git_revision = %S\n" git_revision;
      "let summary = (major, minor , patch , git_revision)\n" 
    ]
  in
  let write_version = Echo (lines, "gobjfs_version.ml") in
  let clean_version =
    match patch with
    | -1 -> git_revision
    | _  -> Printf.sprintf "%i.%i.%i" major minor patch
  in
  let meta_lines = [
      "description = \"gobjfs binding\"\n";
      Printf.sprintf "version = %S\n" clean_version;
      "requires = \"lwt.preemptive, ctypes, ctypes.foreign, ppx_deriving.show\"\n";
      "exists_if = \"libgobjfs.cmxs,libgobjfs.cmxa\"\n";
      "archive(byte) = \"libgobjfs.cma\"\n";
      "archive(native) = \"libgobjfs.cmxa\"\n";
      "linkopts = \"-cclib -lgobjfs_c -cclib -lgobjfs\""
    ]
  in
  let write_meta = Echo (meta_lines, "META") in
  Seq [write_version;write_meta]

      
let _ = dispatch &
          function
          | After_rules ->
             rule "gobjfs_version.ml" ~prod:"gobjfs_version.ml" make_version_and_meta;
             
             dep ["ocaml";"link"]
                 ["gioexec_tools.o";];
             flag ["c";"compile"]
                  (S[A"-ccopt";A"-Wall";
                     A"-ccopt";A"-Wextra";
                     A"-ccopt";A"-Werror";
                     A"-ccopt";A"-ggdb3";
                  ]);
                 
             flag ["link";"ocaml"; "use_gobjfs"]
                  (S[A"-cclib";A"-L../../lib"; (* called from ./_build *)
                     A"-cclib";A"-lgobjfs";
                  ])
                  
          | _ -> ()

                   

