(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Ctypes
open Foreign

let memcmp =
  (* int memcmp(const void *s1, const void *s2, size_t n);
   * do note the int being returned is NOT limited to -1,0,1! *)
  let inner =
    foreign
      "memcmp"
      (ocaml_string
       @-> ocaml_string
       @-> size_t
       @-> returning int)
  in
  fun s1 off1
      s2 off2
      len ->
  inner (ocaml_string_start s1 +@ off1)
        (ocaml_string_start s2 +@ off2)
        (Unsigned.Size_t.of_int len)

let memcmp' =
  let inner =
    foreign
      "memcmp"
      (ocaml_string
       @-> ptr char
       @-> size_t
       @-> returning int)
  in
  fun s1 off1
      (s2 : Lwt_bytes.t) off2
      len ->
  inner (ocaml_string_start    s1 +@ off1)
        (bigarray_start array1 s2 +@ off2)
        (Unsigned.Size_t.of_int len)

let memcmp'' =
  let inner =
    foreign
      "memcmp"
      (ptr char
       @-> ptr char
       @-> size_t
       @-> returning int)
  in
  fun (s1 : Lwt_bytes.t) off1
      (s2 : Lwt_bytes.t) off2
      len ->
  inner (bigarray_start array1 s1 +@ off1)
        (bigarray_start array1 s2 +@ off2)
        (Unsigned.Size_t.of_int len)



let transform_memcmp_output ~len1 ~len2 out =
  match out with
  | 0 ->
     if len1 = len2
     then 0
     else if len1 > len2
     then 1
     else -1
  | r when r > 0 -> 1
  | r (* when r < 0 *) -> -1

let _compare
      memcmp
      s1 off1 len1
      s2 off2 len2
  =
  memcmp s1 off1 s2 off2 (min len1 len2)
  |> transform_memcmp_output ~len1 ~len2

let compare = _compare memcmp
let compare' = _compare memcmp'
let compare'' = _compare memcmp''

let _equal
      compare
      s1 off1 len1
      s2 off2 len2
  =
  if len1 <> len2
  then false
  else (compare s1 off1 len1
                s2 off2 len2) = 0

let equal = _equal compare
let equal' = _equal compare'
let equal'' = _equal compare''
