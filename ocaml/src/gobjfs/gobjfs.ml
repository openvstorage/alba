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

open Foreign
open Ctypes

module GMemPool = struct
  type t = unit Ctypes_static.ptr

  let _gmempool_alloc =
    foreign
      ~release_runtime_lock:false
      "gMempool_alloc"
      (int @-> returning (ptr void))

  let _gmempool_init =
    foreign
      ~release_runtime_lock:false
      "gMempool_init"
      (int @-> returning void)

  let _gmempool_free =
    foreign
      ~release_runtime_lock:false
      "gMempool_free"
      (ptr void @-> returning void)

  let init align_size = _gmempool_init align_size

  let alloc size = _gmempool_alloc size



  let free buffer =
    _gmempool_free buffer
end

module Fragment = struct

  type fragment_t
  type completion_id = int64

  let fragment_t : fragment_t structure typ = structure "fragment"
  let gCompletionID = int64_t

  let completion_id = field fragment_t "completionID" gCompletionID
  let offset = field fragment_t "offset" size_t
  let size   = field fragment_t "size" size_t
  let addr'   = field fragment_t "addr" (ptr void)
  let () = seal fragment_t

  type t = (fragment_t, [ `Struct ]) Ctypes.structured

  let show t = string_of fragment_t t

  let _debug_fragment =
    foreign
      "gobjfs_debug_fragment"
      (ptr fragment_t @-> returning void)


  let to_size = Unsigned.Size_t.of_int

  let make cid off s bytes =
    let t = make fragment_t in
    setf t completion_id cid;
    setf t offset (to_size off);
    setf t size (to_size s);
    setf t addr' bytes;
    (* _debug_fragment (addr t); *)
    t

  let get_bytes t : Lwt_bytes.t =
    let s = getf t size |> Unsigned.Size_t.to_int in
    getf t addr'
    |> from_voidp char
    |> bigarray_of_ptr array1 s Bigarray.Char

  let get_completion_id t = getf t completion_id

  let get_offset t =
    let off = getf t offset in
    Unsigned.Size_t.to_int off

  let get_size t =
    let s = getf t size in
    Unsigned.Size_t.to_int s

  let free_bytes t =
    let bytes = getf t addr' in
    GMemPool.free bytes
end



module Batch = struct
  type _batch
  let _batch : _batch structure typ = structure "gobjfs_batch"
  let _count = field _batch "count" size_t
  let _array = field _batch "array" Fragment.fragment_t (* fragments are 'inlined' *)

  let () = seal _batch

  type t = (_batch, [ `Struct ]) Ctypes.structured

  let show t = string_of _batch t


  let _debug_batch =
    foreign
      "gobjfs_debug_batch"
      (ptr _batch @-> returning void)

  let _alloc =
    foreign
      ~check_errno:false
      ~release_runtime_lock:false
      "gobjfs_batch_alloc"
      (int @-> returning (ptr _batch))

  let make fragments : t =
    let len = List.length fragments in
    if len = 0 then invalid_arg "empty batch";
    let bp = _alloc len in
    let b = !@ bp in
    let a = getf b _array in
    let rec loop ft = function
      | [] -> b
      | fragment :: fragments ->
         (* TODO: signals a bad interface *)

         let copy_attr fn =
           setf ft fn (getf fragment fn)
         in
         copy_attr Fragment.completion_id;
         copy_attr Fragment.offset;
         copy_attr Fragment.size;
         copy_attr Fragment.addr';

         let ft_addr  = addr ft in
         let ft'_addr = ft_addr +@ 1 in
         let ft' = !@ ft'_addr in
         loop ft' fragments
    in
    loop a fragments


end

module Ser = struct
  external get64_prim' : Lwt_bytes.t -> int -> int64 = "%caml_bigstring_get64"
  external get32_prim' : Lwt_bytes.t -> int -> int32 = "%caml_bigstring_get32"
end

module IOExecFile = struct

  type service_handle = unit ptr
  let service_handle : service_handle typ = ptr void

  let _init =
    foreign
      ~check_errno:true
      ~release_runtime_lock:false
      "gobjfs_ioexecfile_service_init"
      (string @-> returning service_handle)

  let init config = _init config

  let _destroy =
    foreign
      ~check_errno:false
      ~release_runtime_lock:false
      "gobjfs_ioexecfile_service_destroy"
      (service_handle @-> returning int32_t)

  let destroy sh =
    let rc = _destroy sh in
    if rc = -1l
    then failwith "destroy"
    else ()

  type _handle
  let _handle : _handle structure typ = structure "gobjfs_handle"
  let _fd   = field _handle "fd" int
  let _core = field _handle "core" int
  let () = seal _handle

  type handle =_handle Ctypes.structure Ctypes_static.ptr
  let show_handle h = string_of (ptr _handle) h


  type _event_channel = unit ptr
  let event_channel : _event_channel typ = ptr void

  type event_channel = _event_channel


  external _convert_open_flags : Unix.open_flag list -> int
    = "gobjfs_ocaml_convert_open_flags"

  let _file_open =
    foreign
      ~check_errno:true
      "gobjfs_ioexecfile_file_open"
      (service_handle @-> string @-> int @-> returning (ptr _handle))

  let file_open service_handle file_name flags =
    let int_val = _convert_open_flags flags in
    let (h:handle) = _file_open service_handle file_name int_val in
    h


  let _file_write =
    foreign
      "gobjfs_ioexecfile_file_write"
      (ptr _handle
       @-> ptr Batch._batch
       @-> ptr void
       @-> returning int32_t)

  let file_write handle batch (event_channel: event_channel) =
    let rc = _file_write handle (addr batch) event_channel in
    if rc = -1l
    then failwith "ioexecfile:file_write"
    else Lwt.return_unit

  let _file_read =
    foreign
      "gobjfs_ioexecfile_file_read"
      (ptr _handle
       @-> ptr Batch._batch
       @-> ptr void
       @-> returning int32_t)

  let file_read handle batch (event_channel: event_channel) =
    let rc = _file_read handle (addr batch) event_channel in
    if rc = -1l
    then failwith "ioexecfile:file_read"
    else Lwt.return_unit

  let _file_close =
    foreign
      ~check_errno:false
      ~release_runtime_lock:false
      "gobjfs_ioexecfile_file_close"
      (ptr _handle @-> returning int32_t)

  let file_close handle =
    let rc = _file_close handle in
    if rc = -1l
    then failwith "ioexecfile::file_close"
    else ()

  let _file_delete =
    foreign
      "gobjfs_ioexecfile_file_delete"
      (service_handle
       @-> string
       @-> int64_t
       @-> ptr void
       @-> returning int32_t)

  let file_delete service_handle name cid event_channel =
    let rc = _file_delete service_handle name cid event_channel in
    if rc = -1l
    then Lwt.fail_with "ioexecfile:file_delete"
    else Lwt.return_unit

  let _open_event_channel =
    foreign
      ~check_errno:false
      ~release_runtime_lock:false
      "gobjfs_ioexecfile_event_fd_open"
      (service_handle @-> returning event_channel)

  let open_event_channel service_handle =
    let r = _open_event_channel service_handle in
    if r = null then failwith "ioexecfile:open_event_channel";
    r


  let _close_event_channel =
    foreign
      ~check_errno:false
      ~release_runtime_lock:false
      "gobjfs_ioexecfile_event_fd_close"
      (event_channel @-> returning int32_t)

  let close_event_channel event_channel =
    let rc = _close_event_channel event_channel in
    if rc = -1l
    then
      Lwt.fail_with "ioexecfile:close_event_channel"
    else Lwt.return_unit

  type status = {
      completion_id : int64;
      error_code: int32;
      reserved: int32;
    } [@@ deriving show]

  let get_completion_id s = s.completion_id

  let get_error_code s = s.error_code

  let _get_read_fd =
    foreign
      "gobjfs_ioexecfile_event_fd_get_read_fd"
      (event_channel @-> returning int)

  let get_reap_fd event_channel =
    let fdi = _get_read_fd event_channel in
    let (fd : Unix.file_descr) = Obj.magic (fdi : int) in
    Lwt_unix.of_unix_file_descr fd

  let reap event_fd (buf:Lwt_bytes.t) =
    let open Lwt.Infix in
    let size = 16 in
    let n = Lwt_bytes.length buf / size in
    let bytes = n * size in
    Lwt_bytes.read event_fd buf 0 bytes >>= fun bytes_read ->

    (if  bytes_read mod size <> 0
     then Lwt.fail_with "bytes_read not a multiple of 16 ?"
     else Lwt.return_unit
    )

    >>= fun ()->

    let rec loop acc off =
      if off = bytes_read then List.rev acc
      else
        let completion_id = Ser.get64_prim' buf off
        and error_code = Ser.get32_prim' buf (off + 8)
        in
        let acc' = {completion_id; error_code; reserved = 0l } :: acc in
        loop acc' (off + size)
    in
    let r = loop [] 0  in
    Lwt.return (bytes_read / size, r)
end
