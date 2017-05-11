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
open Statistics_collection.Generic

let to_buffer_with_version' ~ser_version buf t=
  let module Llio = Llio2.WriteBuffer in
  Llio.int8_to buf ser_version;
  Llio.float_to buf t.creation;
  Llio.float_to buf t.period;
  Llio.hashtbl_to Llio.int32_to Stat_deser.to_buffer' buf t.statistics

let from_buffer_raw' buf =
  let module Llio = Llio2.ReadBuffer in
  let creation = Llio.float_from buf in
  let period   = Llio.float_from buf in
  let statistics =
    let ef buf = Llio.pair_from Llio.int32_from Stat_deser.from_buffer' buf in
    Llio.hashtbl_from ef buf
  in
  { creation; period; statistics}
