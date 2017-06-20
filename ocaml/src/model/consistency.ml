(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
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
