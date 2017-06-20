(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Ctypes
open Static

(* Run this with valgrind *)
type t = (Fsutil.statvfs, [ `Struct ]) Static.structured

let () =
  let open Fsutil in
  let x = free_bytes "/" in
  Printf.printf "free_bytes:%Li\n" x
