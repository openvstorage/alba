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

open Foreign
open Ctypes

(* TODO: make a view for Lwt_unix.file_descr (example below)
   (* val string : string typ *)
      let string =
        view ~read:string_of_char_ptr ~write:char_ptr_of_string (ptr char)
*)

let _syncfs = foreign
                ~check_errno:true
                ~release_runtime_lock:true
                "syncfs" (int @-> returning int)

let syncfs (fd:Unix.file_descr) =
  let (fdi:int) = Obj.magic fd in
  _syncfs fdi

let lwt_syncfs lwt_fd =
  let fd = Lwt_unix.unix_file_descr lwt_fd in
  Lwt_preemptive.detach syncfs fd
