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
