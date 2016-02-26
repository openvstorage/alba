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

module RangeQueryArgs =
  struct
    include Range_query_args.RangeQueryArgs

    (* these serialization functions eventually depend on
     * ctypes, and can't be loaded in the arakoon plugin,
     * hence the separate file... *)

    let from_buffer' order a_from buf =
      let module Llio = Llio2.ReadBuffer in
      let first = a_from buf in
      let finc = Llio.bool_from buf in
      let last =
        Llio.option_from
          (Llio.pair_from
             a_from
             Llio.bool_from)
          buf
      in
      let reverse, max =
        match order with
        | `MaxThenReverse ->
           let max = Llio.int_from buf in
           let reverse = Llio.bool_from buf in
           reverse, max
        | `ReverseThenMax ->
           let reverse = Llio.bool_from buf in
           let max = Llio.int_from buf in
           reverse, max
      in
      { first; finc; last; reverse; max }

    let to_buffer' order a_to buf t =
      let module Llio = Llio2.WriteBuffer in
      let () = a_to buf t.first in
      Llio.bool_to buf t.finc;
      Llio.option_to (Llio.pair_to
                        a_to
                        Llio.bool_to)
                     buf
                     t.last;
      match order with
      | `MaxThenReverse ->
         Llio.int_to buf t.max;
         Llio.bool_to buf t.reverse
      | `ReverseThenMax ->
         Llio.bool_to buf t.reverse;
         Llio.int_to buf t.max
  end
