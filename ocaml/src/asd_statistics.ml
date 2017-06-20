(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

module AsdStatistics = struct
    module G = Statistics_collection.Generic
    type t = G.t
    let section = Alba_statistics.Statistics.section

    let new_delta t code delta = G.new_delta t code delta

    let make () = G.make ()
    let show_inner t to_name= G.show_inner t to_name

    let snapshot t reset = G.snapshot t reset

    let from_buffer' buf =
      let module Llio = Llio2.ReadBuffer in
      let ser_version = Llio.int8_from buf in
      match ser_version with
      | 1 ->
         begin
          let creation  = Llio.float_from buf in
          let period    = Llio.float_from buf in
          let multi_get = Stat_deser.from_buffer' buf in
          let range = Stat_deser.from_buffer' buf in
          let range_entries = Stat_deser.from_buffer' buf in
          let apply = Stat_deser.from_buffer' buf in
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
      | 2 -> Statistics_collection_deser.from_buffer_raw' buf
      | k -> Prelude.raise_bad_tag "AsdStatistics" k

    let to_buffer' buf t =
      Statistics_collection_deser.to_buffer_with_version' ~ser_version:2 buf t

end
