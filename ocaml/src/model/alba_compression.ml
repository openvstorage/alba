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

open Prelude

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
