(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

module Compression = struct
  type t =
    | NoCompression
    | Snappy
    | Bzip2
    | Test
  [@@deriving show, yojson]

  let output buf c =
    let t = match c with
      | NoCompression -> 1
      | Snappy -> 2
      | Bzip2  -> 3
      | Test   -> 4
    in
    Llio.int8_to buf t

  let input buf =
    match Llio.int8_from buf with
    | 1 -> NoCompression
    | 2 -> Snappy
    | 3 -> Bzip2
    | 4 -> Test
    | k -> raise_bad_tag "Compression" k

end
