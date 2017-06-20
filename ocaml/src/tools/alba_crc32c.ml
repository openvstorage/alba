(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

external crc32c_string : int32 -> string -> int -> int -> bool -> int32
  = "crc32c_string"

external crc32c_bigarray:
  int32 ->
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
      -> int -> int -> bool -> int32
  = "crc32c_bigarray"

module Crc32c = struct
    let string ?(crc=0xFFFFFFFFl) str off len final =
      crc32c_string crc str off len final

    let big_array ?(crc=0xFFFFFFFFl) ba off len final =
      crc32c_bigarray crc ba off len final
end
