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

open Checksum.Checksum

let to_buffer' buf =
  let module Llio = Llio2.WriteBuffer in
  function
  | NoChecksum ->
     Llio.int8_to buf 1
  | Sha1 d ->
     Llio.int8_to buf 2;
     assert (String.length d = 20);
     Llio.string_to buf d
  | Crc32c d ->
     Llio.int8_to buf 3;
     Llio.int32_to buf d

let from_buffer' buf =
  let module Llio = Llio2.ReadBuffer in
  match Llio.int8_from buf with
  | 1 -> NoChecksum
  | 2 ->
     let d = Llio.string_from buf in
     assert(String.length d = 20);
     Sha1 d
  | 3 ->
     let d = Llio.int32_from buf in
     Crc32c d
  | k -> Prelude.raise_bad_tag "checksum" k

let deser' = from_buffer', to_buffer'
