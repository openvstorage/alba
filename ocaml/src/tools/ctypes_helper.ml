(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

open Prelude
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
