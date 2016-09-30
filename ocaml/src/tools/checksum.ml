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

module Checksum = struct
  module Algo = struct
    type t =
      | NO_CHECKSUM
      | SHA1
      | CRC32c
          [@@deriving show]

    let to_buffer buf = function
      | NO_CHECKSUM -> Llio.int8_to buf 1
      | SHA1 -> Llio.int8_to buf 2
      | CRC32c -> Llio.int8_to buf 3

    let from_buffer buf =
      match Llio.int8_from buf with
      | 1 -> NO_CHECKSUM
      | 2 -> SHA1
      | 3 -> CRC32c
      | k -> Prelude.raise_bad_tag "Checksum.Algo" k
  end

  type algo = Algo.t [@@deriving show]

  type t =
    | NoChecksum
    | Sha1 of HexString.t (* a string of size 20, actually *)
    | Crc32c of HexInt32.t
  [@@deriving show]

  let output buf = function
    | NoChecksum ->
      Llio.int8_to buf 1
    | Sha1 d ->
      Llio.int8_to buf 2;
      assert (String.length d = 20);
      Llio.string_to buf d
    | Crc32c d ->
      Llio.int8_to buf 3;
      Llio.int32_to buf d

  let input buf =
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

  let deser = input, output
  let from_buffer, to_buffer = input, output

  let algo_of =
    let open Algo in
    function
    | NoChecksum -> NO_CHECKSUM
    | Sha1 _ -> SHA1
    | Crc32c _ -> CRC32c
end
