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
open Arakoon_client

type t = consistency =
  | Consistent
  | No_guarantees
  | At_least of Stamp.t

let from_buffer buf =
  match Llio.int8_from buf with
  | 0 -> Consistent
  | 1 -> No_guarantees
  | 2 -> At_least (Llio.int64_from buf)
  | k -> raise_bad_tag "Consistency" k

let to_buffer buf = function
  | Consistent -> Llio.int8_to buf 0
  | No_guarantees -> Llio.int8_to buf 1
  | At_least t ->
    Llio.int8_to buf 2;
    Llio.int64_to buf t
