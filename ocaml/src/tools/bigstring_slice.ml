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

open Lwt_bytes2

type t = {
  bs : Lwt_bytes.t;
  offset : int;
  length : int;
}

let from_bigstring bs offset length =
  assert (offset + length <= Lwt_bytes.length bs);
  { bs; offset; length; }

let wrap_bigstring bs =
  from_bigstring bs 0 (Lwt_bytes.length bs)

let create length =
  wrap_bigstring (Lwt_bytes.create length)

let create_random len =
  wrap_bigstring (Lwt_bytes.create_random len)

let ptr_start t =
  let open Ctypes in
  bigarray_start array1 t.bs +@ t.offset

let extract_to_bigstring t =
  Lwt_bytes.extract t.bs t.offset t.length

let extract t offset length =
  let bs = Lwt_bytes.extract t.bs (t.offset + offset) length in
  wrap_bigstring bs

let length t = t.length

let get t pos = Lwt_bytes.get t.bs (t.offset + pos)
let set t pos c = Lwt_bytes.set t.bs (t.offset + pos) c

let to_string t =
  let s = Bytes.create t.length in
  Lwt_bytes.blit_to_bytes t.bs t.offset s 0 t.length;
  s

let of_string s =
  Lwt_bytes.of_string s |> wrap_bigstring

let show (t:t) =
  if t.length < 32
  then Printf.sprintf "<bigstring_slice: length=%i %S>" t.length (to_string t)
  else Printf.sprintf "<bigstring_slice: length=%i _ >" t.length


let pp formatter t =
  Format.pp_print_string formatter (show t)
