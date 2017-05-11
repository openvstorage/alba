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

open! Prelude

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

    let deser' order (a_from, a_to) = from_buffer' order a_from, to_buffer' order a_to
  end
