(*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
