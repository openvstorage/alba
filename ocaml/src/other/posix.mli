(*
 * This file is part of Baardskeerder.
 *
 * Copyright (C) 2011 Incubaid BVBA
 *
 * Baardskeerder is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Baardskeerder is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Baardskeerder.  If not, see <http://www.gnu.org/licenses/>.
 *)

(* we (OVS) only took what we needed and adapted the signatures a bit *)

(* posix_fadvise *)
type posix_fadv = POSIX_FADV_NORMAL
                  | POSIX_FADV_SEQUENTIAL
                  | POSIX_FADV_RANDOM
                  | POSIX_FADV_NOREUSE
                  | POSIX_FADV_WILLNEED
                  | POSIX_FADV_DONTNEED

val posix_fadvise: Unix.file_descr -> int -> int -> posix_fadv -> unit

(** fallocate fd mode offset len :
    The  default  operation (i.e., mode is zero) of fallocate() 
    allocates the disk space within the range specified by 
    offset and len.
*)                                                                   
val fallocate: Unix.file_descr -> int -> int -> int -> unit
                                                         
val lwt_posix_fadvise: Lwt_unix.file_descr -> int -> int -> posix_fadv -> unit Lwt.t

val lwt_fallocate: Lwt_unix.file_descr -> int -> int -> int -> unit Lwt.t
