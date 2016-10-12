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
    let write_scenario (oc:Lwt_io.output_channel) progress =
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
        oc progress
        write
        count
    in
    let partial_read_scenario oc progress =
      let target = Lwt_bytes.create partial_read_size in
      B.measured_loop
        oc progress
        (fun fnr ->
         D.with_blob_fd
           dir_info
           (Int64.of_int fnr)
           (fun fd ->
            let ufd = Lwt_unix.unix_file_descr fd in
            Posix.posix_fadvise ufd 0 value_size Posix.POSIX_FADV_RANDOM;
            Lwt_extra2.read_all_lwt_bytes_exact fd target 0 partial_read_size))
        count
    in
    let oc = Lwt_io.stdout in

    Lwt_list.iter_s
      (fun scenario ->
        let step = count / 100 in
        let progress = B.make_progress step in
        let f =
          (match snd scenario with
           | `Writes -> write_scenario
           | `PartialReads -> partial_read_scenario)
        in
        f oc progress
        >>= fun r ->
        B.report oc (fst scenario) r)
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
