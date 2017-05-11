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

(* These functions live here and not in stat.ml
 * because otherwise we can't load the plugins
 * into arakoon
 *)

open! Prelude
open Stat.Stat

let to_buffer' buf stat =
  let module Llio = Llio2.WriteBuffer in
  Llio.int64_to buf stat.n;
  Llio.float_to buf stat.avg;
  Llio.float_to buf stat.exp_avg;
  Llio.float_to buf stat.m2;
  Llio.float_to buf stat.var;
  Llio.float_to buf stat.min;
  Llio.float_to buf stat.max;
  Llio.float_to buf stat.alpha
let from_buffer' buf =
  let module Llio = Llio2.ReadBuffer in
  let n       = Llio.int64_from buf in
  let avg     = Llio.float_from buf in
  let exp_avg = Llio.float_from buf in
  let m2      = Llio.float_from buf in
  let var     = Llio.float_from buf in
  let min     = Llio.float_from buf in
  let max     = Llio.float_from buf in
  let alpha   = Llio.float_from buf in
  { n;avg;exp_avg;m2;var;min;max; alpha}
