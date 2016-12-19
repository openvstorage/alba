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

let client_file_prefix default =
  let doc =
    "prepend the client output files with $(docv)"
    ^  " to keep multiple processes out of each other's way"
  in
  Arg.(value
       & opt string default
       & info ["client-file-prefix"] ~docv:"CLIENT_FILE_PREFIX" ~doc
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

let seeds =
  let open Arg in
  value
  & opt_all int []
  & info ["seed"]
         ~docv:"SEED"
         ~doc:"starting seeds for random number generators for clients (may be repeated)"

let adjust_seeds n_clients seeds =
  let len = List.length seeds in
  let remainder = n_clients - len in
  if remainder > 0
  then
    let seeds_extra =
      let rec loop acc i =
        if i = remainder
        then acc
        else loop (42 :: acc) (i+1)
      in
      loop [] 0
    in
    seeds @ seeds_extra
  else
    seeds
