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

open Lwt.Infix

class type reader = object
  method reset : unit Lwt.t
  (* returns amount of bytes read + has_more  *)
  method read : int -> Lwt_bytes.t -> (int * bool) Lwt.t
end

class file_reader fd size = (object
  val mutable pos = 0

  method reset =
    pos <- 0;
    Lwt.map ignore (Lwt_unix.lseek fd 0 Lwt_unix.SEEK_SET)

  method read cnt target =
    Lwt_extra2.read_all_lwt_bytes fd target 0 cnt >>= fun read ->
    pos <- pos + read;
    Lwt.return (read, pos < size)
end : reader)

class string_reader object_data = (object
  val obj_len = String.length object_data
  val mutable pos = 0

  method reset =
    pos <- 0;
    Lwt.return_unit

  method read cnt target =
    let n' =
      if pos + cnt > obj_len
      then obj_len - pos
      else cnt
    in
    Lwt_bytes.blit_from_bytes object_data pos target 0 n';
    pos <- pos + n';
    Lwt.return (n', pos < obj_len)
end : reader)

class bytes_reader object_data = (object
  val obj_len = Lwt_bytes.length object_data
  val mutable pos = 0

  method reset =
    pos <- 0;
    Lwt.return_unit

  method read n target =
    let n' =
      if pos + n > obj_len
      then obj_len - pos
      else n
    in
    Lwt_bytes.blit object_data pos target 0 n';
    pos <- pos + n';
    Lwt.return (n', pos < obj_len)
end : reader)
