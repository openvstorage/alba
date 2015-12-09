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

open Cmdliner
open Cli_common
open Prelude

let n_clients default =
  let doc = "number of concurrent clients for the benchmark" in
  Arg.(value
       & opt int default
       & info ["n-clients"] ~docv:"N_CLIENTS" ~doc)

let n default =
  let doc = "do runs (writes,reads,partial_reads,...) of $(docv) iterations" in
  Arg.(value
       & opt int default
       & info ["n"; "nn"] ~docv:"N" ~doc)

let power default =
  let doc = "$(docv) for random number generation: period = 10^$(docv)" in
  Arg.(value
       & opt int default
       & info ["power"] ~docv:"power" ~doc
  )

let prefix default =
  let doc = "$(docv) to keep multiple clients out of each other's way" in
  Arg.(value
       & opt string default
       & info ["prefix"] ~docv:"prefix" ~doc
  )

let slice_size default =
  let doc = "partial reads phaze will use slices of size $(docv)" in
  Arg.(value
       & opt int default
       & info ["slice-size"] ~docv:"SLICE_SIZE"  ~doc
  )

let scenarios =
  let scns = [ "writes", `Writes;
               "reads", `Reads;
               "partial-reads", `Partial_reads;
               "deletes", `Deletes; ] in
  Arg.(value
       & opt_all
           (enum scns)
           []
       & info [ "scenario" ]
              ~doc:(Printf.sprintf
                      "choose which scenario to run, valid values are %s"
                      ([%show : string list] (List.map fst scns))))

let robust =
  Arg.(value
       & flag
       & info ["robust"] ~docv:"robust writes (with retry loop)")

let map_scenarios scenarios robust =
  let open Proxy_bench in
  List.map
    (function
      | `Writes -> do_writes ~robust
      | `Reads  -> do_reads
      | `Partial_reads -> do_partial_reads
      | `Deletes -> do_deletes)
    (if scenarios = []
     then [ `Writes;
            `Reads;
            `Partial_reads;
            `Deletes; ]
     else scenarios)

let proxy_bench host port
                n_clients (n:int) file_name (power:int)
                prefix (slice_size:int) namespace_name
                scenarios robust
  =
  lwt_cmd_line
    false
    (fun () ->
     Proxy_bench.do_scenarios
       host port
       n_clients n
       file_name power prefix slice_size namespace_name
       (map_scenarios scenarios robust))

let proxy_bench_cmd =
  Term.(pure proxy_bench
        $ host $ port 10000
        $ n_clients 1
        $ n 10000
        $ file_upload 1
        $ power 4
        $ prefix ""
        $ slice_size 4096
        $ namespace 0
        $ scenarios
        $ robust
  ),
  Term.info "proxy-bench" ~doc:"simple proxy benchmark"

let alba_bench alba_cfg_file tls_config
               n_clients n file_name power
               prefix slice_size namespace
               scenarios robust
  =
  lwt_cmd_line
    false
    (fun () ->
     Alba_bench.do_scenarios
       (ref
          (Albamgr_protocol.Protocol.Arakoon_config.from_config_file
             alba_cfg_file))
       ~tls_config
       n_clients n
       file_name power prefix slice_size namespace
       (map_scenarios scenarios robust))

let alba_bench_cmd =
  Term.(pure alba_bench
        $ alba_cfg_file
        $ tls_config
        $ n_clients 1
        $ n 10000
        $ file_upload 1
        $ power 4
        $ prefix ""
        $ slice_size 4096
        $ namespace 0
        $ scenarios
        $ robust),
  Term.info "alba-bench" ~doc:"simple alba benchmark"

let cmds = [
    proxy_bench_cmd;
    alba_bench_cmd;
  ]
