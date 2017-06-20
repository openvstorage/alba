(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

let to_namespace_name prefix = function
  | 0 -> fun namespace_id ->
         prefix ^ (serialize ~buf_size:4 x_int64_be_to namespace_id)
  | 1 -> Printf.sprintf "%s_%09Li" prefix
  | _ -> assert false
