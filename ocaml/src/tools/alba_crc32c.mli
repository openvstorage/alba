(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

module Crc32c : sig
    val string:
      ?crc:int32 -> string -> int -> int -> bool -> int32

    val big_array :
      ?crc:int32 ->
      (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
      -> int -> int -> bool -> int32
end
