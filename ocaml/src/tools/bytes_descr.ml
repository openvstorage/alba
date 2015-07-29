(*
Copyright 2015 Open vStorage NV

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
open Slice
open Ctypes

module Bytes_descr = struct
  type ('t, 'c) t =
    | Slice : (Slice.t, string Ctypes_static.ocaml) t
    | Bigarray : (Lwt_bytes.t, char Ctypes_static.ptr) t

  let get_ctypes : type t' c'. (t', c') t -> c' Ctypes.typ = function
    | Slice -> ocaml_string
    | Bigarray -> ptr char

  let length : type t' c'. (t', c') t -> t' -> int = function
    | Slice -> Slice.length
    | Bigarray -> Lwt_bytes.length

  let create : type t' c'. (t', c') t -> int -> t' = function
    | Slice -> fun len -> Slice.wrap_string (Bytes.create len)
    | Bigarray -> Lwt_bytes.create

  let start : type t' c'. (t', c') t -> t' -> c' = function
    | Slice -> fun sl ->
      let open Slice in
      ocaml_string_start sl.buf +@ sl.offset
    | Bigarray -> bigarray_start array1

  let sub : type t' c'. (t', c') t -> t' -> int -> int -> t' = function
    | Slice -> Slice.sub
    | Bigarray -> Lwt_bytes.proxy

  let set32_prim : type t' c'. (t', c') t -> t' -> int -> int32 -> unit = function
    | Slice -> fun sl off ->
      set32_prim sl.Slice.buf (off + sl.Slice.offset)
    | Bigarray -> set32_prim'

  let get32_prim : type t' c'. (t', c') t -> t' -> int -> int32 = function
    | Slice -> fun sl off ->
      get32_prim sl.Slice.buf (off + sl.Slice.offset)
    | Bigarray -> get32_prim'
end
