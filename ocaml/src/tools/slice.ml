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

open Prelude

module Slice = struct
  type t = { buf : Bytes.t;
             offset : int;
             length : int; }

  let make buf offset length =
    assert (offset + length <= Bytes.length buf);
    { buf; offset; length }
  let create length =
    make (Bytes.create length) 0 length
  let wrap_string buf =
    make buf 0 (String.length buf)
  let wrap_bytes = wrap_string

  let get_string t = String.sub t.buf t.offset t.length

  let get_string_unsafe t =
    if t.offset = 0 && t.length = String.length t.buf
    then t.buf
    else get_string t

  let show t = String.escaped (get_string_unsafe t)

  let pp formatter t =
    Format.pp_print_string formatter (show t)

  let show_limited_escaped s =
    if s.length < 50
    then Printf.sprintf "%S" (get_string_unsafe s)
    else "..."

  let pp_limited_escaped formatter t =
    Format.pp_print_string formatter (show_limited_escaped t)

  let to_buffer buffer t =
    Llio.int_to buffer t.length;
    Buffer.add_substring buffer t.buf t.offset t.length

  let from_buffer buffer =
    let length = Llio.int_from buffer in
    let offset = buffer.Llio.pos in
    buffer.Llio.pos <- offset + length;
    make buffer.Llio.buf offset length

  let add_prefix_byte_as_bytes t c =
      let b = Bytes.create (t.length + 1) in
      Bytes.set b 0 c;
      Bytes.blit_string t.buf t.offset b 1 t.length;
      b

  let with_prefix_byte_unsafe t c f =
    let off = t.offset in
    if off < 1
    then f (wrap_string (add_prefix_byte_as_bytes t c))
    else begin
      let off_1 = off - 1 in
      let c = t.buf.[off_1] in
      Bytes.set t.buf off_1 c;
      finalize
        (fun () ->
           f (make t.buf off_1 (t.length + 1)))
        (fun () ->
           Bytes.set t.buf off_1 c)
    end

  let sub t offset length =
    make t.buf (t.offset + offset) length

  let length t = t.length

  let _memcmp =
    let open Ctypes in
    let open Foreign in
    (* int memcmp(const void *s1, const void *s2, size_t n);
       do note then int being returned is NOT limited to -1,0,1! *)
    let inner =
      foreign
        "memcmp"
        (ocaml_string @-> ocaml_string @-> size_t @-> returning int)
    in
    fun
      string1 offset1 length1
      string2 offset2 length2 ->
      inner
        (ocaml_string_start string1 +@ offset1)
        (ocaml_string_start string2 +@ offset2)
        (Unsigned.Size_t.of_int (min length1 length2))

  let compare_substrings s1 off1 len1 s2 off2 len2 =
    match _memcmp s1 off1 len1 s2 off2 len2 with
    | 0 -> 0
    | r when r > 0 -> 1
    | r when r < 0 -> -1
    | _ -> failwith "ocaml pattern matching ain't too smart"

  let compare s1 s2 =
    compare_substrings
      s1.buf s1.offset s1.length
      s2.buf s2.offset s2.length

  let compare' = CompareLib.wrap compare

  let to_bigstring t =
    let res = Lwt_bytes.create t.length in
    Lwt_bytes.blit_from_bytes t.buf t.offset res 0 t.length;
    res

  let of_bigstring bs =
    let s = Lwt_bytes.to_string bs in
    wrap_string s
end
