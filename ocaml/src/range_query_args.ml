(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

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
