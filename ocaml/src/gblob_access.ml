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
  let _throw_ex_from ec function_name par =
    (* TODO: do we need to be specific ? *)
    let error = Unix.EUNKNOWNERR (Int32.to_int ec) in
    let ex =Unix.Unix_error(error, function_name, par) in
    Lwt.fail ex
  in
  let _process_results get_ec function_name get_par results =
    let bad = List.filter (fun t -> get_ec t <> 0l) results in
    match bad with
    | [] -> Lwt.return_unit
    | t ::_  -> _throw_ex_from (get_ec t) function_name  (get_par t)
  in
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
  val _outstanding =  (Hashtbl.create 16 : (int64, int32 Lwt.u) Hashtbl.t)
  val _event_fd = IOExecFile.get_event_fd ()

  method private next_completion_id =
    let r = !_next_id in
    let () = _next_id := Int64.succ r in
    r

  method config = config

  method get_blob fnr len =
    let fn = self # _get_file_path fnr in
    let completion_id = self # next_completion_id in
    IOExecFile.file_open fn [Unix.O_RDONLY] >>= fun handle ->
    let bytes = GMemPool.alloc len in
    Lwt.finalize
      (fun () ->
        let fragment = Fragment.make completion_id 0 len bytes in
        let fragments = [ fragment ] in
        let batch = Batch.make fragments in
        IOExecFile.file_read handle batch >>= fun () ->
        self # _wait_for_completion completion_id >>= fun ec ->
        begin
          if ec <> 0l
          then _throw_ex_from ec "get_blob" fn
          else Lwt.return_unit
        end
        >>= fun () ->
        (* TODO: don't blit *)
        let src = Fragment.get_bytes fragment in
        let tgt = Bytes.create len in
        Lwt_bytes.blit_to_bytes src 0 tgt 0 len;
        Lwt.return tgt
      )
      (fun () ->
        IOExecFile.file_close handle >>= fun () ->
        GMemPool.free bytes; (* Fragment.free_bytes fragment bytes *)
        Lwt.return_unit
      )


  method _push_blob_data fnr len slices _f =
    Lwt_log.debug_f "push_blob_data fnr:%Li len:%i slices:%si"
                    fnr len ([%show : (int * int) list] slices)
    >>= fun () ->
    let fn = self # _get_file_path fnr in
    IOExecFile.file_open fn [Unix.O_RDONLY] >>= fun handle ->
    let fragments =
      List.map
        (fun (off,len) ->
          let completion_id = self # next_completion_id in
          let bytes = GMemPool.alloc len in
          let fragment = Fragment.make completion_id off len bytes in
          fragment
        ) slices
    in
    let batch = Batch.make fragments in

    Lwt.finalize
      (fun () ->
        IOExecFile.file_read handle batch >>= fun () ->
        Lwt_list.map_p
          (fun fragment ->
            self # _wait_for_completion (Fragment.get_completion_id fragment) >>= fun ec ->
            Lwt.return (fragment, ec)
          )
          fragments
        >>= fun results ->
        _process_results
          (fun (_,ec) -> ec)
          "IOExecFile.file_read"
          (fun _ -> fn)
          results
        >>= fun () ->
        Lwt_list.iter_s _f results
      )
      (fun () ->
        List.iter Fragment.free_bytes fragments;
        IOExecFile.file_close handle
      )

  method push_blob_data fnr len slices f =
    let _f (fragment,ec) =
      let offset = Fragment.get_offset fragment
      and size   = Fragment.get_size   fragment
      and buffer = Fragment.get_bytes  fragment
      in
      f (offset, size) buffer 0 size
    in
    self # _push_blob_data fnr len slices _f

  method send_blob_data_to fnr len slices nfd =
    let _f (fragment,ec) =
      let size   = Fragment.get_size  fragment
      and buffer = Fragment.get_bytes fragment
      in
      Net_fd.write_all_lwt_bytes nfd buffer 0 size
    in
    self # _push_blob_data fnr len slices _f


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
          let awake = Hashtbl.find _outstanding completion_id in
          Lwt.wakeup awake ec;
          Hashtbl.remove _outstanding completion_id
        ) ss;
      loop ()
    in
    loop ()

  method _wait_for_completion completion_id : int32 Lwt.t =
    let sleep, awake = Lwt.wait () in
    let () = Hashtbl.add _outstanding completion_id awake in
    sleep

  method write_blob fnr blob ~post_write ~sync_parent_dirs =

    let dir, _, file_path = self # _get_file_dir_name_path fnr in
    self # ensure_dir_exists dir ~sync:sync_parent_dirs >>= fun () ->

    let completion_id = self # next_completion_id  in
    Lwt_log.debug_f "write_blob ~fnr:%Li completion_id:%Li" fnr completion_id >>= fun () ->
    let len = Blob.length blob in
    let bytes = GMemPool.alloc len in
    let fragment = Fragment.make completion_id 0 len bytes in

    (* TODO: don't blit *)
    let tgt = Fragment.get_bytes fragment in
    let src = blob |> Blob.get_bigslice in
    Lwt_bytes.blit src.Bigstring_slice.bs 0 tgt 0 len;


    let fragments = [fragment] in
    let batch = Batch.make fragments in
    IOExecFile.file_open
      file_path
      [Unix.O_WRONLY;Unix.O_CREAT;Unix.O_SYNC]
    >>= fun handle ->

    Lwt.finalize
      (fun () ->
        IOExecFile.file_write handle batch >>= fun () ->
        self # _wait_for_completion completion_id >>= fun ec ->
        Lwt_log.debug_f "%Li:write ec=%li" completion_id ec >>= fun () ->
        if ec <> 0l
        then _throw_ex_from ec "write_blob" file_path
        else Lwt.return_unit
      )
      (fun () ->
        GMemPool.free bytes;
        IOExecFile.file_close handle >>= fun () ->
        let parent_dir = config.files_path ^ "/" ^ dir in
        post_write None len parent_dir >>= fun () ->
        Lwt_log.debug_f "write_blob finalizer done"
      )

  method delete_blobs fnrs ~ignore_unlink_error =
    Lwt_log.debug_f "delete_blobs ~fnrs:%s" ([%show: int64 list] fnrs) >>= fun () ->
    let fnrs = List.sort Int64.compare fnrs in

    Lwt_list.map_s
      (fun fnr ->
        let file_path = self # _get_file_path fnr in
        let cid = self # next_completion_id in
        IOExecFile.file_delete file_path cid >>= fun () ->
        Lwt.return (fnr, file_path, cid)
      )
      fnrs
    >>= fun completions ->
    Lwt_list.map_p
      (fun (fnr, file_path, cid) ->
        self # _wait_for_completion cid >>= fun ec ->
        Lwt.return (fnr, file_path, ec)
      )
      completions
    >>= fun results ->
    if ignore_unlink_error
    then Lwt.return_unit
    else
      _process_results (fun (_, _, ec) -> ec)
                       "delete_blob"
                       (fun (_,fnr , _) -> fnr) results

  method _get_file_dir_name_path fnr = Fnr.get_file_dir_name_path config.files_path fnr
end
