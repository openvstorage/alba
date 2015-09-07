(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
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

  let algo_of =
    let open Algo in
    function
    | NoChecksum -> NO_CHECKSUM
    | Sha1 _ -> SHA1
    | Crc32c _ -> CRC32c
end
