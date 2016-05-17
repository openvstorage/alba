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
