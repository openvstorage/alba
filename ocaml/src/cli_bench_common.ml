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

open Cmdliner


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

let value_size default =
  let doc = "do sets of $(docv) bytes" in
  Arg.(
    value
    & opt int default
    & info ["v"; "value-size"] ~docv:"V" ~doc
  )

let partial_fetch_size default =
  let doc = "do partial reads of $(docv) bytes" in
  Arg.(
    value
    & opt int default
    & info ["v-partial";] ~doc
  )
