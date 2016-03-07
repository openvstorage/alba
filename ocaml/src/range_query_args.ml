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
    type 'a t = {
        first : 'a;
        finc : bool;
        last : ('a * bool) option;
        reverse : bool;
        max : int;
      } [@@deriving show]

    let to_buffer order a_to buf { first; finc; last; max; reverse; } =
      a_to buf first;
      Llio.bool_to buf finc;
      Llio.option_to (Llio.pair_to a_to Llio.bool_to) buf last;
      match order with
      | `MaxThenReverse ->
         Llio.int_to buf max;
         Llio.bool_to buf reverse
      | `ReverseThenMax ->
         Llio.bool_to buf reverse;
         Llio.int_to buf max

    let from_buffer order a_from buf =
      let first = a_from buf in
      let finc = Llio.bool_from buf in
      let last = Llio.option_from (Llio.pair_from a_from Llio.bool_from) buf in
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
      { first; finc; last; max; reverse; }
  end
