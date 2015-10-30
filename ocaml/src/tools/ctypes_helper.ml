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
