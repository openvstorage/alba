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
open Alba_crc32c
open OUnit


let to_hex d =
  let size = String.length d in
  let r_size = size * 2 in
  let result = Bytes.create r_size in
  for i = 0 to (size - 1) do
    Bytes.blit (Printf.sprintf "%02x" (int_of_char d.[i])) 0 result (2*i) 2;
  done;
  result

let printer i32 =
  let rs = Bytes.create 4 in
  set32_prim rs 0 i32;
  Printf.sprintf "{%S}" (to_hex rs)

let tests = [
    "", 0x0l;
    "The quick brown fox jumps over the lazy dog", 0x22620404l;

    (* https://tools.ietf.org/html/rfc3720#appendix-B.4 *)
    String.make 32 '\000', 0x8a9136aal; (* aa 36 91 8a *)
    String.make 32 '\255', 0x62a8ab43l; (* 43 ab a8 62 *)
    String.init 32 (fun i -> Char.chr i) , 0x46dd794el;(* 4e 79 dd 46 *)
    String.init 32 (fun i -> Char.chr (31 -i)), 0x113fdb5cl; (* 5c db 3f 11 *)
    (*"\x00\x00", 0x01l; to see the msg & printer at work *)
  ]




let test_string() =
  let test_s (s, e) =
    let len = String.length s in
    let crc = Crc32c.string s 0 len true in
    OUnit.assert_equal e crc ~printer ~msg:(String.escaped s)
  in
  List.iter test_s tests


type ba =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout)
    Bigarray.Array1.t

let from_string s =
  let len = String.length s in
  let (ba:ba) = Bigarray.Array1.create Bigarray.Char Bigarray.c_layout len in
  let () = String.iteri (Bigarray.Array1.set ba) s in
  ba

let test_bigarray() =
  let test1 (s, e) =
    let ba = from_string s in
    let len = String.length s in
    let crc = Crc32c.big_array ba 0 len true in
    OUnit.assert_equal e crc ~printer ~msg:(String.escaped s)
  in
  List.iter test1 tests


let suite =
  [
    "string"  >:: test_string;
    "bigarray">:: test_bigarray;
  ]
