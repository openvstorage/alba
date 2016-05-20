open Foreign
open Ctypes
open Lwt.Infix

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
  let off_t = size_t
  
  let completion_id = field fragment_t "completionID" gCompletionID
  let offset = field fragment_t "offset" off_t
  let size   = field fragment_t "size" size_t
  let addr'   = field fragment_t "addr" (ptr void)
  let () = seal fragment_t

  type t = (fragment_t, [ `Struct ]) Ctypes.structured

  let show t = string_of fragment_t t
    
                (*
  let set_bytes t  (bytes:Lwt_bytes.t)  =
    setf t addr (to_voidp (bigarray_start array1 bytes))
                 *)
  let _debug_fragment =
    foreign
      "gobjfs_debug_fragment"
      (ptr fragment_t @-> returning void)
  
  
  let to_size = Unsigned.Size_t.of_int
                  
  let make cid off s =
    let t = make fragment_t in
    setf t completion_id cid;
    setf t offset (to_size off);
    setf t size (to_size s);
    let bytes = GMemPool.alloc s in
    setf t addr' bytes;
    _debug_fragment (addr t);
    t

  let get_bytes t : Lwt_bytes.t =
    let s = getf t size |> Unsigned.Size_t.to_int in
    getf t addr'
    |> from_voidp char
    |> bigarray_of_ptr array1 s Bigarray.Char
    
  
end
                    

module Batch = struct
  type _batch
  let _batch : _batch structure typ = structure "gobjfs_batch"
  let count = field _batch "count" size_t
  let array = field _batch "array" Fragment.fragment_t
                    
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
    match fragments with
    | [fragment] ->
       begin         
         let bp = _alloc 1 in
         let b = !@ bp in
         _debug_batch bp;
         let a = getf b array in
         (* TODO: this is a bad interface *)
         let copy_attr fn =
           setf a fn (getf fragment fn)
         in
         copy_attr Fragment.completion_id;
         copy_attr Fragment.offset;
         copy_attr Fragment.size;
         copy_attr Fragment.addr';
         _debug_batch bp;
         b
       end
    | _ -> failwith "todo:Batch.make"
         
end

module Ser = struct
  external get64_prim' : Lwt_bytes.t -> int -> int64 = "%caml_bigstring_get64"
  external get32_prim' : Lwt_bytes.t -> int -> int32 = "%caml_bigstring_get32"
end

module IOExecFile = struct
  let _init =
    foreign
      ~check_errno:true
      ~release_runtime_lock:false
      "gobjfs_ioexecfile_service_init"
      (string @-> returning int32_t)
      
  let init config = _init config |> ignore

  let _destroy =
    foreign
      ~check_errno:false
      ~release_runtime_lock:false
      "gobjfs_ioexecfile_service_destroy"
      (void @-> returning int32_t)

  let destroy () =
    let rc = _destroy () in
    if rc = -1l
    then failwith "destroy"
    else ()

  type _handle
  let _handle : _handle structure typ = structure "gobjfs_handle"
  let fd   = field _handle "fd" int
  let core = field _handle "core" int
  let () = seal _handle

  type handle =_handle Ctypes.structure Ctypes_static.ptr

  let show_handle h = string_of (ptr _handle) h
                                
  external _convert_open_flags : Unix.open_flag list -> int
    = "gobjfs_ocaml_convert_open_flags"
        
  let _file_open =
    foreign
      ~check_errno:true
      "gobjfs_ioexecfile_file_open"
      (string @-> int @-> returning (ptr _handle))

  let file_open file_name flags =
    let int_val = _convert_open_flags flags in
    let (h:handle) = _file_open file_name int_val in
    Lwt.return h


  let _file_write =
    foreign
      "gobjfs_ioexecfile_file_write"
      (ptr _handle @-> ptr Batch._batch @-> ptr void @->returning int32_t)

  let file_write handle batch =
    Lwt_log.debug_f "file_write ~handle:%s batch:%s"
                    (show_handle handle) (Batch.show batch)
    >>= fun () ->
    let rc = _file_write handle (addr batch) null in
    Lwt_log.debug_f "file_write ~handle:%s rc=%li" (show_handle handle) rc
    >>= fun () ->
    
    if rc = -1l
    then failwith "ioexecfile:file_write"
    else Lwt.return_unit

  let _file_read =
    foreign
      "gobjfs_ioexecfile_file_read"
      (ptr _handle @-> ptr Batch._batch @-> ptr void @-> returning int32_t)

  let file_read handle batch =
    let rc = _file_read handle (addr batch) null in
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
    else Lwt.return_unit

  let _file_delete =
    foreign
      "gobjfs_ioexecfile_file_delete"
      (string @-> int64_t @-> returning int32_t)
      
  let file_delete name cid =
    let rc = _file_delete name cid in
    if rc = -1l
    then failwith "ioexecfile:file_delete"
    else Lwt.return_unit

  let _get_event_fd =
    foreign
      ~check_errno:false
      ~release_runtime_lock:false
      "gobjfs_ioexecfile_get_event_fd"
      (ptr int @-> returning int32_t)

  type fd = Lwt_unix.file_descr
              
  let get_event_fd () =
    let fd = allocate int 0 in
    let rc = _get_event_fd fd in
    
    if rc = -1l
    then failwith "ioexecfile:get_event_fd"
    else let v = (!@ fd) in
         let (fd:Unix.file_descr) = Obj.magic v in
         Lwt_unix.of_unix_file_descr fd

  type status = {
      completion_id : int64;
      error_code: int32;
      reserved: int32;
    } [@@ deriving show]

  let get_completion_id s = s.completion_id

  let get_error_code s = s.error_code

  let reap event_fd (buf:Lwt_bytes.t) =
    let open Lwt.Infix in
    let size = 16 in
    let n = Lwt_bytes.length buf / size in
    let bytes = n * size in
    Lwt_bytes.read event_fd buf 0 bytes >>= fun bytes_read ->
    Lwt_log.debug_f "bytes_read:%i%!" bytes_read >>= fun () ->
    assert (bytes_read mod size = 0);
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

(*
module Version = struct
  include Gobjfs_version
end
 *)
