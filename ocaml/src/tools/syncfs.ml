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
