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

open Prelude
open Slice
open Lwt_bytes2
open Ctypes

module Bytes_descr = struct
  type ('t, 'c) t =
    | Slice : (Slice.t, string Ctypes_static.ocaml) t
    | Bigarray : (Lwt_bytes.t, char Ctypes_static.ptr) t
    | Bigstring_slice : (Bigstring_slice.t, char Ctypes_static.ptr) t

  let get_ctypes : type t' c'. (t', c') t -> c' Ctypes.typ = function
    | Slice -> ocaml_string
    | Bigarray -> ptr char
    | Bigstring_slice -> ptr char

  let length : type t' c'. (t', c') t -> t' -> int = function
    | Slice -> Slice.length
    | Bigarray -> Lwt_bytes.length
    | Bigstring_slice -> Bigstring_slice.length

  let create : type t' c'. (t', c') t -> int -> t' = function
    | Slice -> fun len -> Slice.wrap_string (Bytes.create len)
    | Bigarray -> Lwt_bytes.create
    | Bigstring_slice -> Bigstring_slice.create

  let start : type t' c'. (t', c') t -> t' -> c' = function
    | Slice -> fun sl ->
      let open Slice in
      ocaml_string_start sl.buf +@ sl.offset
    | Bigarray -> bigarray_start array1
    | Bigstring_slice -> Bigstring_slice.ptr_start

  let start_with_offset : type t' c'. (t', c') t -> t' -> int -> c' =
    function
    | Slice -> fun t offset -> (start Slice t) +@ offset
    | Bigarray -> fun t offset -> (start Bigarray t) +@ offset
    | Bigstring_slice -> fun t offset ->
                         start Bigstring_slice t +@ offset


  let extract : type t' c'. (t', c') t -> t' -> int -> int -> t' =
    function
    | Slice -> Slice.sub
    | Bigarray -> fun t offset len ->
                  let res = Lwt_bytes.extract t offset len in
                  Lwt_bytes.unsafe_destroy t;
                  res
    | Bigstring_slice -> Bigstring_slice.extract

  let set32_prim : type t' c'. (t', c') t -> t' -> int -> int32 -> unit = function
    | Slice -> fun sl off ->
      set32_prim sl.Slice.buf (off + sl.Slice.offset)
    | Bigarray -> set32_prim'
    | Bigstring_slice ->
       fun t off ->
       set32_prim'
         t.Bigstring_slice.bs
         (t.Bigstring_slice.offset + off)

  let get32_prim : type t' c'. (t', c') t -> t' -> int -> int32 = function
    | Slice -> fun sl off ->
      get32_prim sl.Slice.buf (off + sl.Slice.offset)
    | Bigarray -> get32_prim'
    | Bigstring_slice ->
       fun t off ->
       get32_prim'
         t.Bigstring_slice.bs
         (off + t.Bigstring_slice.offset)
end
