(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

type t = {
  bs : Lwt_bytes.t;
  offset : int;
  length : int;
}

let from_bigstring bs offset length =
  assert (offset + length <= Lwt_bytes.length bs);
  { bs; offset; length; }

let from_shared_buffer sb offset length =
  let bs = SharedBuffer.deref sb in
  assert (offset + length <= Lwt_bytes.length bs);
  { bs; offset; length; }

let wrap_bigstring bs =
  from_bigstring bs 0 (Lwt_bytes.length bs)

let wrap_shared_buffer sb =
  let b = SharedBuffer.deref sb in
  from_bigstring b 0 (Lwt_bytes.length b)

let create ?msg length =
  wrap_bigstring (Lwt_bytes.create ?msg length)

let create_random ?msg len =
  wrap_bigstring (Lwt_bytes.create_random ?msg len)

let ptr_start t =
  let open Ctypes in
  bigarray_start array1 t.bs +@ t.offset

let extract_to_bigstring ?msg t =
  Lwt_bytes.extract ?msg t.bs t.offset t.length

let extract t offset length =
  let bs = Lwt_bytes.extract t.bs (t.offset + offset) length in
  wrap_bigstring bs

let blit t offset length dest dest_off =
  Lwt_bytes.blit t.bs (t.offset + offset)
                 dest dest_off
                 length

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
