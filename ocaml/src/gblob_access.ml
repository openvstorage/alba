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
open Asd_statistics

class g_directory_info statistics config service_handle =
  let _throw_ex_from ec function_name par =
    (* TODO: do we need to be specific ? *)
    let error = Unix.EUNKNOWNERR (- Int32.to_int ec) in
    let ex =Unix.Unix_error(error, function_name, par) in
    Lwt.fail ex
  in

  let _process_results get_ec function_name get_par results =
    let bad = List.filter (fun t -> get_ec t <> 0l) results in
    match bad with
    | [] -> Lwt.return_unit
    | t ::_  -> _throw_ex_from (get_ec t) function_name  (get_par t)
  in

  let _event_channel = IOExecFile.open_event_channel service_handle in
  let _event_fd  = IOExecFile.get_event_fd _event_channel in
  let alignment = 4096 in

object(self)
  inherit blob_access statistics
  inherit default_directory_access config.files_path


  initializer
    begin
      Lwt.ignore_result (self # _inner ())
    end

  val mutable _next_id = 0L
  val _outstanding =  (Hashtbl.create 16 : (int64, int32 Lwt.u) Hashtbl.t)

  method private next_completion_id =
    let completion_id = _next_id in
    let () = _next_id <- Int64.succ completion_id in
    let sleep, awake = Lwt.wait () in
    let () = Hashtbl.add _outstanding completion_id awake in
    (completion_id, sleep)

  method config = config

  method get_blob fnr len =
    let fn = self # _get_file_path fnr in
    let completion_id,sleep = self # next_completion_id in

    let handle = IOExecFile.file_open service_handle fn [Unix.O_RDONLY] in
    let len' =
          let remainder = len mod alignment in
          if remainder = 0
          then len
          else len + (alignment - remainder)
        in
    let bytes = GMemPool.alloc len' in

    Lwt.finalize
      (fun () ->
        let fragment = Fragment.make completion_id 0 len' bytes in
        let fragments = [ fragment ] in
        let batch = Batch.make fragments in
        IOExecFile.file_read handle batch _event_channel >>= fun () ->
        sleep >>= fun ec ->
        begin
          if ec <> 0l
          then _throw_ex_from ec "get_blob" fn
          else Lwt.return_unit
        end
        >>= fun () ->
        (* TODO: don't blit, but this is only used
                 - in a rare path: get blob from fs to compare with expectation
                 - in multiget1 : which is deprecated only used by antique proxies
         *)
        let src = Fragment.get_bytes fragment in
        let tgt = Bytes.create len in
        Lwt_bytes.blit_to_bytes src 0 tgt 0 len;
        Lwt.return tgt
      )
      (fun () ->
        let () = IOExecFile.file_close handle in
        GMemPool.free bytes;
        Lwt.return_unit
      )


  method _get_blob_data fnr len slices _f =
    let fn = self # _get_file_path fnr in

    let took, handle  =
      Prelude.with_timing
        (fun () ->IOExecFile.file_open service_handle fn [Unix.O_RDONLY] )
    in
    AsdStatistics.new_delta _statistics _FILE_OPEN took;

    let corrections =
      List.map
        (fun (off,len) ->
          let completion_id,sleep = self # next_completion_id in
          (*
              (for now) gobjfs does not support arbitrary partial reads
              but offset and len need to be multiples of 4096

              |---- left ------- | ----- len ----- |----- right -----|
             off'               off             off+len          off'+len'

           *)

          let left = off mod alignment in
          let off' =
            if left = 0
            then off
            else off - left
          in
          let remainder = (left + len) mod alignment in
          let len' =
            if remainder = 0
            then left + len
            else left + len + (alignment - remainder)
          in
          let bytes = GMemPool.alloc len' in
          let fragment = Fragment.make completion_id off' len' bytes in
          (completion_id, off', off, len, len', fragment, sleep)
        ) slices
    in
    let fragments =
      List.map
        (fun (completion_id, off', off, len, len', fragment,sleep) -> fragment)
        corrections
    in
    let batch = Batch.make fragments in

    Lwt.finalize
      (fun () ->
        Prelude.with_timing_lwt
          (fun () ->
            IOExecFile.file_read handle batch _event_channel >>= fun () ->
            Lwt_list.map_p
              (fun correction ->
                let completion_id, _, _, _,_, _, sleep = correction in
                Prelude.with_timing_lwt (fun () -> sleep)
                >>= fun (took,ec) ->
                let () = AsdStatistics.new_delta _statistics _READ_WAIT took in
                Lwt.return (correction, ec)
              )
              corrections
          )
        >>= fun (took,results) ->

        let () = AsdStatistics.new_delta _statistics _READ_BATCH took in

        _process_results
          (fun (_,ec) -> ec)
          "IOExecFile.file_read"
          (fun _ -> fn)
          results
        >>= fun () ->
        Prelude.with_timing_lwt
          (fun () -> Lwt_list.iter_s _f results)
        >>= fun (took,()) ->
        let () = AsdStatistics.new_delta _statistics _READ_BATCH_CB took in
        Lwt.return_unit
      )
      (fun () ->
        List.iter Fragment.free_bytes fragments;
        let took, () = Prelude.with_timing
          (fun () ->IOExecFile.file_close handle)
        in
        AsdStatistics.new_delta _statistics _FILE_CLOSE took;
        Lwt.return_unit
      )

  method get_blob_data fnr len slices f =
    let _f (correction,ec) =
      let (completion_id, off',off, len, len',fragment, sleep) = correction in
      let buffer = Fragment.get_bytes  fragment in
      let left = off - off' in
      f (off, len) buffer left
    in
    self # _get_blob_data fnr len slices _f

  method send_blob_data_to fnr len slices nfd =
    let _f (correction, ec) =
      let (completion_id,off',off, len,len',fragment, sleep) = correction in
      let buffer = Fragment.get_bytes fragment in
      let left = off' - off in
      Net_fd.write_all_lwt_bytes nfd buffer left len
    in
    self # _get_blob_data fnr len slices _f


  method private _inner () : unit Lwt.t =
    Lwt_log.debug "_inner ()" >>= fun () ->
    let buf = Lwt_bytes.create 256 in
    let rec loop () =
      Lwt_unix.wait_read _event_fd  >>= fun () ->
      IOExecFile.reap _event_fd buf >>= fun (n, ss) ->
      (*Lwt_log.debug_f "reaped:%i" n >>= fun () -> *)
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
    Lwt.catch
      loop
      (fun exn -> Lwt_log.fatal ~exn "_inner loop died")



  method write_blob fnr blob ~post_write ~sync_parent_dirs =

    let dir, _, file_path = self # _get_file_dir_name_path fnr in
    self # ensure_dir_exists dir ~sync:sync_parent_dirs >>= fun () ->


    let len = Blob.length blob in
    let remainder = len mod alignment in
    if remainder <> 0
    then
      begin
        Lwt_log.debug_f
          "ragged blob size: fnr:%Li len:%i (0x%0x) using ordinary write"
          fnr len len
        >>= fun () ->
        Lwt_extra2.with_fd
          file_path
          ~flags:Lwt_unix.([ O_WRONLY; O_CREAT; O_EXCL; ])
          ~perm:0o664
          (fun fd ->
            let extra = alignment - remainder in
            let padded_size = len + extra  in
            Lwt.finalize
              (fun () ->
                maybe_fallocate config fd len >>= fun () ->
                Blob.write_blob blob fd >>= fun () ->
                Lwt_unix.ftruncate fd padded_size >>= fun () ->
                (* you need to sync here:
                   otherwise you might read garbage later on as
                   the real data still resides in some buffers
                  *)
                Lwt_unix.fsync fd
              )
              (fun () ->
                let parent_dir = config.files_path ^ "/" ^ dir in
                post_write (Some fd) len parent_dir >>= fun () ->
                let ufd = Lwt_unix.unix_file_descr fd in
                maybe_fadvise_dont_need config ufd len
              )
          )
      end
    else
      begin
        let bytes = GMemPool.alloc len in
        let completion_id, sleep = self # next_completion_id  in
        Lwt_log.debug_f "write_blob ~fnr:%Li completion_id:%Li" fnr completion_id >>= fun () ->
        let fragment = Fragment.make completion_id 0 len bytes in

        (* TODO: don't blit *)
        let tgt = Fragment.get_bytes fragment in
        let src = blob |> Blob.get_bigslice in
        let open Bigstring_slice in
        Lwt_bytes.blit
          src.bs src.offset
          tgt 0
          len;


        let fragments = [fragment] in
        let batch = Batch.make fragments in
        let handle = IOExecFile.file_open
          service_handle
          file_path
          [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_SYNC]
        in

        Lwt.finalize
          (fun () ->
            IOExecFile.file_write handle batch _event_channel >>= fun () ->
            sleep >>= fun ec ->
            Lwt_log.debug_f "%Li:write ec=%li" completion_id ec >>= fun () ->
            if ec <> 0l
            then _throw_ex_from ec "write_blob" file_path
            else Lwt.return_unit
          )
          (fun () ->
            GMemPool.free bytes;
            let () = IOExecFile.file_close handle in
            let parent_dir = config.files_path ^ "/" ^ dir in
            post_write None len parent_dir >>= fun () ->
            Lwt_log.debug_f "write_blob finalizer done"
          )
      end

  method delete_blobs fnrs ~ignore_unlink_error =
    Lwt_log.debug_f "delete_blobs ~fnrs:%s" ([%show: int64 list] fnrs) >>= fun () ->
    let fnrs = List.sort Int64.compare fnrs in

    Lwt_list.map_s
      (fun fnr ->
        let file_path = self # _get_file_path fnr in
        let completion_id, sleep = self # next_completion_id in
        IOExecFile.file_delete service_handle file_path completion_id _event_channel
        >>= fun () ->
        Lwt.return (fnr, file_path, completion_id, sleep)
      )
      fnrs
    >>= fun completions ->
    Lwt_list.map_p
      (fun (fnr, file_path, completion_id, sleep) ->
        sleep >>= fun ec ->
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
