(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)


open! Prelude

include Llio

let varint_to buf i =
  let rec loop i =
    if i < 128
    then int8_to buf i
    else
      let b = (i land 0x7f) lor 0x80 in
      let () = int8_to buf b in
      loop (i lsr 7)
  in
  loop i

let varint_from buf =
  let rec loop r shift b0 =
    if b0 < 0x80
    then r + (b0 lsl shift)
    else
      let r' = r + ((b0 land 0x7f) lsl shift)  in
      let b0' = int8_from buf in
      loop r' (shift + 7) b0'
  in
  let b0 = int8_from buf in
  loop 0 0 b0

let small_bytes_to buf s =
  varint_to buf (Bytes.length s);
  raw_string_to buf s

let small_bytes_from buf =
  let len = varint_from buf in
  raw_string_from len buf
