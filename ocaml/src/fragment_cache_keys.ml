(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

let make_key ~object_id ~chunk_id ~fragment_id =
  serialize
    (Llio.tuple3_to
       Llio.string_to
       Llio.int_to
       Llio.int_to)
    (object_id, chunk_id, fragment_id)
