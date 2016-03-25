(*
Copyright 2016 iNuron NV

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

open Lwt_bytes2
open Lwt.Infix
open Cmdliner
open Cli_common
open Cli_bench_common


(* TODO bench_asd_no_network *)
(* TODO bench 1 big file *)


let bench_blobs path scenarios count value_size partial_read_size =
  let t () =
    let module D = Asd_server.DirectoryInfo in
    let module B = Generic_bench in
    let dir_info =
      D.make
        ~use_fadvise:true
        ~use_fallocate:true
        path
    in
    let write_scenario progress =
      let blob = Lwt_bytes.create_random value_size in
      let blob = Asd_protocol.Blob.Lwt_bytes blob in
      let write fnr =
        D.write_blob
          dir_info
          (Int64.of_int fnr) blob
          ~post_write:(fun fd len parent_dir -> Lwt.return ())
          ~sync_parent_dirs:true
      in
      B.measured_loop
        progress
        write
        count
    in
    let partial_read_scenario progress =
      let target = Lwt_bytes.create partial_read_size in
      B.measured_loop
        progress
        (fun fnr ->
         D.with_blob_fd
           dir_info
           (Int64.of_int fnr)
           (fun fd ->
            Lwt_unix.lseek fd 0 Lwt_unix.SEEK_SET >>= fun _ ->
            Lwt_extra2.read_all_lwt_bytes_exact fd target 0 partial_read_size))
        count
    in
    Lwt_list.iter_s
      (fun scenario ->
       let progress = B.make_progress (count/100) in
       (match snd scenario with
        | `Writes -> write_scenario
        | `PartialReads -> partial_read_scenario)
         progress >>= fun r ->
       B.report (fst scenario) r)
      scenarios
  in
  lwt_cmd_line ~to_json:false ~verbose:false t

let bench_blobs_cmd =
  Term.(pure bench_blobs
        $ Arg.(required
               & pos 0 (some file) None
               & info [] ~docv:"PATH")
        $ Arg.(
          let scns = [ "writes", `Writes;
                       "partial_reads", `PartialReads;
                     ] in
          let scns' = List.map
                        (fun (name, v) ->
                         name, (name, v))
                        scns
          in
          value
          & opt_all (enum scns') scns
          & info [ "scenario" ]
                 ~doc:(Printf.sprintf
                         "choose which scenario to run, valid values are %s"
                         ([%show : string list] (List.map fst scns)))
          )
        $ n 10000
        $ value_size 1_000_000
        $ partial_fetch_size 4096
  ),
  Term.info "asd-bench-blobs"


let cmds = [
    bench_blobs_cmd;
  ]
