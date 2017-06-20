(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Ctypes
open Foreign

let free =
  foreign
    "free"
    (ptr void @-> returning void)
let free pt = free (to_voidp pt)

let with_free f g =
  let a = f () in
  finalize
    (fun () -> g a)
    (fun () -> free a)

let with_free_lwt f g =
  let a = f () in
  Lwt.finalize
    (fun () -> g a)
    (fun () ->
     free a;
     Lwt.return ())
