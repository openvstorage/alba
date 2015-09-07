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
