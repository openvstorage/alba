(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
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
