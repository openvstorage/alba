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

open Prelude
open Lwt_bytes2
open Lwt.Infix
open Cmdliner
open Cli_common
open Cli_bench_common


(* TODO bench_asd_no_network *)
(* TODO bench 1 big file *)


let bench_blobs path scenarios count value_size partial_read_size read_method =
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
      let t = ref 0. in
      let target = Lwt_bytes.create partial_read_size in
      B.measured_loop
        progress
        (fun fnr ->
         D.with_blob_fd
           ~async_method:`Synchronous
           dir_info
           (Int64.of_int fnr)
           (fun fd ->
            with_timing_lwt
              (fun () ->
               let ufd = Lwt_unix.unix_file_descr fd in
               match read_method with
               | `Aio ->
                  Posix.add_odirect ufd;
                  Aio_lwt.(pread default_context
                                 fd 0 partial_read_size) >>= fun bss ->
                  Lwt_bytes.unsafe_destroy bss.Bigstring_slice.bs;
                  Lwt.return_unit
               | `Read ->
                  Posix.posix_fadvise ufd 0 value_size Posix.POSIX_FADV_RANDOM;
                  Lwt_extra2.read_all_lwt_bytes_exact fd target 0 partial_read_size >>= fun () ->
                  Posix.posix_fadvise ufd 0 value_size Posix.POSIX_FADV_DONTNEED;
                  Lwt.return_unit
              ) >>= fun (t', ()) ->
            t := !t +. t';
            Lwt.return_unit
           ))
        count >>= fun r ->
      Lwt_log.info_f "t = %f" !t >>= fun () ->
      Lwt.return r
    in
    let partial_read_scenario2 progress =
      let rec with_fds f acc = function
        | [] -> f acc
        | fnr :: fnrs ->
           D.with_blob_fd
             dir_info
             (Int64.of_int fnr)
             (fun fd ->
              Posix.add_odirect (Lwt_unix.unix_file_descr fd);
              with_fds f (fd :: acc) fnrs)
      in
      let fnrs = ref [] in
      B.measured_loop
        Lwt.return
        (fun i -> fnrs := i :: !fnrs;
                  Lwt.return_unit)
        count >>= fun _ ->
      with_fds
        (fun fds ->
         let fds = ref fds in
         B.measured_loop
           progress
           (fun i ->
            let fd = List.hd_exn !fds in
            fds := List.tl_exn !fds;
            Aio_lwt.(pread default_context
                           fd 0 partial_read_size) >>= fun bss ->
            Lwt_bytes.unsafe_destroy bss.Bigstring_slice.bs;
            Lwt.return_unit
           )
           count)
        []
        !fnrs
    in
    Lwt_list.iter_s
      (fun scenario ->
       let progress = B.make_progress (count/100) in
       (match snd scenario with
        | `Writes -> write_scenario
        | `PartialReads -> partial_read_scenario
        | `PartialReads2 -> partial_read_scenario2)
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
                       "partial_reads2", `PartialReads2;
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
        $ Arg.(
          let methods = [ "aio", `Aio;
                          "read", `Read;
                        ] in
          value
          & opt (enum methods) `Aio
          & info [ "read-method"]
                 ~doc:(Printf.sprintf
                         "choose which read method to use for the partial read scenario (%s)"
                         ([%show : string list] (List.map fst methods)))
          )
  ),
  Term.info "asd-bench-blobs"


let cmds = [
    bench_blobs_cmd;
  ]
