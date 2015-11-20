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

open Checksum.Checksum

let to_buffer' buf =
  let module Llio = Llio2.WriteBuffer in
  function
  | NoChecksum ->
     Llio.int8_to buf 1
  | Sha1 d ->
     Llio.int8_to buf 2;
     assert (String.length d = 20);
     Llio.string_to buf d
  | Crc32c d ->
     Llio.int8_to buf 3;
     Llio.int32_to buf d

let from_buffer' buf =
  let module Llio = Llio2.ReadBuffer in
  match Llio.int8_from buf with
  | 1 -> NoChecksum
  | 2 ->
     let d = Llio.string_from buf in
     assert(String.length d = 20);
     Sha1 d
  | 3 ->
     let d = Llio.int32_from buf in
     Crc32c d
  | k -> Prelude.raise_bad_tag "checksum" k
