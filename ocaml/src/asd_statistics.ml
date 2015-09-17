(*
Copyright 2015 Open vStorage NV

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

open Stat

module AsdStatistics = struct
    module G = Statistics_collection.Generic
    type t = G.t
    let section = Alba_statistics.Statistics.section
    let with_timing = G.with_timing
    let with_timing_lwt = G.with_timing_lwt

    let new_delta t code delta = G.new_delta t code delta

    let make () = G.make ()
    let show_inner t to_name= G.show_inner t to_name

    let snapshot t reset = G.snapshot t reset

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      match ser_version with
      | 1 ->
         begin
          let creation  = Llio.float_from buf in
          let period    = Llio.float_from buf in
          let multi_get = Stat.stat_from buf in
          let range = Stat.stat_from buf in
          let range_entries = Stat.stat_from buf in
          let apply = Stat.stat_from buf in
          let statistics = Hashtbl.create 16 in
          let () = List.iter
            (fun (c,stat) ->
             Hashtbl.add statistics c stat)
            [
              (1l, range);
              (2l, multi_get);
              (3l, apply);
              (4l, range_entries);
            ];
          in
          {
              G.creation ;
              G.period ;
              G.statistics
          }
         end
      | 2 -> Statistics_collection.Generic.from_buffer_raw buf
      | _ -> failwith "bad serialization version"

    let to_buffer buf t =
      Statistics_collection.Generic.to_buffer_with_version ~ser_version:2 buf t

end
