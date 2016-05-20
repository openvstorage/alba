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

open Blob_access
open Lwt.Infix
       
open Gobjfs
open Asd_protocol

class g_directory_info gioexecfile config =

object(self)
  inherit default_directory_access config.files_path
  inherit blob_access

  initializer
    begin
      IOExecFile.init gioexecfile;
      let align_size = 4096 in
      GMemPool.init align_size;
      Lwt.ignore_result (self # _inner ())
    end
                                        
  val _next_id = ref 0L
  val _outstanding =  (Hashtbl.create 16 : (int64, (Fragment.t * int32 Lwt.u)) Hashtbl.t)
  val _event_fd = IOExecFile.get_event_fd ()
                    
  method private next_completion_id =
    let r = !_next_id in
    let () = _next_id := Int64.succ r in
    r
      
  method config = config
                    
  method get_blob fnr len =
    let fn = self # _get_file_path fnr in
    let completion_id = self # next_completion_id in
    let fragment = Fragment.make completion_id 0 len in
    let fragments = [ fragment ] in
    let batch = Batch.make fragments in
    IOExecFile.file_open fn [Unix.O_RDONLY] >>= fun handle ->
    IOExecFile.file_read handle batch >>= fun () ->

    self # _wait_for_completion completion_id fragment >>= fun ec ->
    assert (ec = 0l);
    IOExecFile.file_close handle >>= fun () ->
    failwith "todo: end of get_blob"
    
  method with_blob_fd fnr f = failwith "todo:with_blob_fd"

  method private _inner () : unit Lwt.t =
    Lwt_log.debug "_inner ()" >>= fun () ->
    let buf = Lwt_bytes.create 256 in
    let rec loop () =
      Lwt_unix.wait_read _event_fd  >>= fun () ->
      IOExecFile.reap _event_fd buf >>= fun (n, ss) ->
      Lwt_log.debug_f "reaped:%i" n >>= fun () ->
      List.iter
        (fun s ->
          let completion_id = IOExecFile.get_completion_id s in
          let ec = IOExecFile.get_error_code s in
          let fragment, awake = Hashtbl.find _outstanding completion_id in
          Lwt.wakeup awake ec;
          Hashtbl.remove _outstanding completion_id
        ) ss;
      loop ()
    in
    loop ()

  method _wait_for_completion completion_id fragment : int32 Lwt.t =
    let sleep, awake = Lwt.wait () in
    let () = Hashtbl.add _outstanding completion_id (fragment, awake) in
    sleep 
      
  method write_blob fnr blob ~post_write ~sync_parent_dirs =
    
    let dir, _, file_path = self # _get_file_dir_name_path fnr in
    self # ensure_dir_exists dir ~sync:sync_parent_dirs >>= fun () ->
    
    let completion_id = self # next_completion_id  in
    Lwt_log.debug_f "write_blob ~fnr:%Li completion_id:%Li" fnr completion_id >>= fun () ->
    let len = Blob.length blob in
    let fragment = Fragment.make completion_id 0 len in

    (* TODO: don't blit *)
    let tgt = Fragment.get_bytes fragment in
    let src = blob |> Blob.get_bigslice in
    Lwt_bytes.blit src.Bigstring_slice.bs 0 tgt 0 len;

    
    let fragments = [fragment] in
    let batch = Batch.make fragments in
    IOExecFile.file_open file_path [Unix.O_WRONLY;Unix.O_CREAT;Unix.O_SYNC] >>= fun handle ->
    Lwt.finalize
      (fun () ->
        IOExecFile.file_write handle batch >>= fun () ->
        self # _wait_for_completion completion_id fragment >>= fun ec ->
        Lwt_log.debug_f "%Li:write ec=%li" completion_id ec >>= fun () ->
        (* TODO: post write ?? *)
        if ec <> 0l
        then
          Lwt.fail_with (Printf.sprintf "IOExecFile.write:ec=%li" ec)
        else Lwt.return_unit
      )
      (fun () ->
        (* IOExecFile.free data *)
        IOExecFile.file_close handle >>= fun () ->
        Lwt_log.debug_f "write_blob finalizer done"
      )
             
  method delete_blobs fnrs ~ignore_unlink_error = failwith "todo"
  
  method _get_file_dir_name_path fnr = Fnr.get_file_dir_name_path config.files_path fnr
end
