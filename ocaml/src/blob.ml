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

open Slice

module Blob = struct
  type t =
    | Slice of Slice.t         (* TODO show limited escaped variant... *)
    | Lwt_bytes of Lwt_bytes.t

  let slice s = Slice s

  let show = function
    | Slice _ -> "Slice ..."
    | Lwt_bytes _ -> "Lwt_bytes ..."

  let length = function
    | Slice s -> Slice.length s
    | Lwt_bytes s -> Lwt_bytes.length s

  let pp formatter t = Format.pp_print_string formatter (show t)

  type t' =
    | Direct of Slice.t
    | Later of int              (* size *)
                 [@@deriving show]

  let to_slice = function
    | Slice s -> s
    | Lwt_bytes s -> Slice.wrap_string (Lwt_bytes.to_string s)

  let to_string_unsafe = function
    | Slice s -> Slice.get_string_unsafe s
    | Lwt_bytes s -> Lwt_bytes.to_string s

  let to_lwt_bytes = function
    | Slice s -> Slice.to_bigstring s
    | Lwt_bytes s -> s

  let to_buffer1 buf b =
    Slice.to_buffer buf (to_slice b)

  let to_buffer' buf = function
    | Direct value ->
       Llio.int8_to buf 1;
       Slice.to_buffer buf value
    | Later size ->
       Llio.int8_to buf 2;
       Llio.int_to buf size
  let to_buffer2 write_later buf b =
    to_buffer'
      buf
      (match b with
       | Slice value -> Direct value
       | Lwt_bytes bs ->
          let () = write_later bs in
          Later (Lwt_bytes.length bs))

  let from_buffer' buf =
    match Llio.int8_from buf with
    | 1 ->
       let value = Slice.from_buffer buf in
       Direct value
    | 2 ->
       let size = Llio.int_from buf in
       Later size
    | k -> Prelude.raise_bad_tag "Asd_server.Value.blob" k

  let from_buffer2 buf =
    let x = from_buffer' buf in
    fun read_bytes ->
    match x with
    | Direct s -> Lwt.return (Slice s)
    | Later size -> read_bytes size
end
