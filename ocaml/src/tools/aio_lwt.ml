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

open Lwt.Infix
open Lwt_bytes2

let make_context n = Aio.context n

let init context =
  let fd = Aio.fd context in
  let lwt_fd = Lwt_unix.of_unix_file_descr
                 ~blocking:false
                 ~set_flags:true
                 fd
  in
  let rec inner () =
    Lwt_unix.wait_read lwt_fd >>= fun () ->
    let () = Aio.process context in
    inner ()
  in
  Lwt.ignore_result (inner ())

let page_size = Aio.Buffer.page_size ()

let cast_buffer_to_lwt_bytes (bs : Aio.Buffer.t) : Lwt_bytes.t =
  Obj.magic bs

let pread context fd offset length =
  let t, u = Lwt.wait () in
  let start_page = offset / page_size in
  let start_page_offset = start_page * page_size in
  let start_offset_in_page = offset - start_page_offset in
  let end_page = (offset + length - 1) / page_size in
  let pages = end_page - start_page + 1 in
  let buf = Aio.Buffer.create (pages * page_size) in
  let buf' = cast_buffer_to_lwt_bytes buf in
  Lwt.catch
    (fun () ->
     let () =
       Aio.read
         context
         fd
         (Int64.of_int start_page_offset)
         buf
         (let open Aio in
          function
          | Result buf -> Lwt.wakeup u ()
          | Errno err -> Lwt.wakeup_exn u (Aio.Error err)
          | Partial (buf, got) ->
             if got = start_offset_in_page + length
             then Lwt.wakeup u () (* read up to the bytes we're interested in *)
             else Lwt.wakeup_exn
                    u
                    (Failure (Printf.sprintf
                                "aio pread partial error (got %i, offset=%i, length=%i)"
                                got offset length)))
     in
     t >>= fun () ->
     let res =
       Bigstring_slice.from_bigstring
         buf'
         start_offset_in_page
         length
     in
     Lwt.return res)
    (fun exn ->
     Lwt_bytes.unsafe_destroy buf';
     Lwt.fail exn)

let default_context =
  let r = make_context 1024 in
  let () = init r in
  r
