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

open Prelude
open Lwt_buffer
open Lwt.Infix

type 'a input = (unit -> ('a * int) Lwt.t) * 'a Lwt.u
type output_blobs = (int64 * Asd_protocol.Blob.t) list * unit Lwt.u
type delete_blobs = unit * unit Lwt.u
type sync_rocks   = unit Lwt.u

type post_write = Lwt_unix.file_descr -> int -> string -> unit Lwt.t
type 'a t = {
    high_prio_reads  : 'a input     Lwt_buffer.t;
    high_prio_writes : output_blobs Lwt_buffer.t;
    high_prio_deletes: delete_blobs Lwt_buffer.t;
    low_prio_reads   : 'a input     Lwt_buffer.t;
    low_prio_writes  : output_blobs Lwt_buffer.t;
    low_prio_deletes : delete_blobs Lwt_buffer.t;

    rocksdb_sync     : sync_rocks   Lwt_buffer.t;

    sync_rocksdb     : unit -> unit;

    write_blob       : int64 -> Asd_protocol.Blob.t ->
                       post_write ->
                       unit Lwt.t;
  }

let make sync_rocksdb write_blob =
  { high_prio_reads  = Lwt_buffer.create ();
    high_prio_writes = Lwt_buffer.create ();
    high_prio_deletes= Lwt_buffer.create ();
    low_prio_reads   = Lwt_buffer.create ();
    low_prio_writes  = Lwt_buffer.create ();
    low_prio_deletes = Lwt_buffer.create ();
    rocksdb_sync     = Lwt_buffer.create ();
    sync_rocksdb;
    write_blob;
  }

type priority = Asd_protocol.Protocol.priority =
              | High
              | Low

let perform_read t prio r =
  let sleep, waker = Lwt.wait () in
  Lwt_buffer.add
    (r, waker)
    (match prio with
     | High -> t.high_prio_reads
     | Low  -> t.low_prio_reads) >>= fun () ->
  sleep

let perform_write t prio blobs =
  let sleep, waker = Lwt.wait () in
  Lwt_buffer.add
    (blobs, waker)
    (match prio with
     | High -> t.high_prio_writes
     | Low  -> t.low_prio_writes) >>= fun () ->
  sleep

let perform_delete t prio =
  let sleep, waker = Lwt.wait () in
  Lwt_buffer.add
    ((), waker)
    (match prio with
     | High -> t.high_prio_deletes
     | Low  -> t.low_prio_deletes) >>= fun () ->
  sleep

let sync_rocksdb t =
  let sleep, waker = Lwt.wait () in
  Lwt_buffer.add
    waker
    t.rocksdb_sync >>= fun () ->
  sleep

let rec _harvest_from_buffer buffer acc cost threshold get_item_cost =
  if cost >= threshold || not (Lwt_buffer.has_item buffer)
  then Lwt.return (acc, cost)
  else
    begin
      Lwt_buffer.take buffer >>= fun (item, waiter) ->
      Lwt.catch
        (fun () ->
         get_item_cost item >>= fun (res, cost) ->
         Lwt.return (Result.Ok res, cost))
        (fun exn ->
         Lwt.return (Result.Error exn, 1000)) >>= fun (res, cost') ->
      _harvest_from_buffer
        buffer
        ((waiter, res) :: acc)
        (cost + cost')
        threshold
        get_item_cost
    end

let _notify_waiters res =
  List.iter
    (function
      | (waker, Result.Ok res) -> Lwt.wakeup_later waker res
      | (waker, Result.Error exn) -> Lwt.wakeup_later_exn waker exn)
    res

let _perform_reads_from_buffer buffer threshold =
  _harvest_from_buffer buffer [] 0 threshold (fun f -> f ()) >>= fun (results, _) ->
  _notify_waiters results;
  Lwt.return_unit

let _perform_deletes_from_buffer buffer count =
  _harvest_from_buffer
    buffer
    [] 0 count
    (fun () -> Lwt.return ((), 1))
  >>= fun (results, _) ->
  _notify_waiters results;
  Lwt.return_unit

let run t ~fsync ~fs_fd =
  let rec inner () =
    Lwt.pick
      [ Lwt_buffer.wait_for_item t.high_prio_writes;
        Lwt_buffer.wait_for_item t.high_prio_reads;
        Lwt_buffer.wait_for_item t.high_prio_deletes;
        Lwt_buffer.wait_for_item t.low_prio_writes;
        Lwt_buffer.wait_for_item t.low_prio_reads;
        Lwt_buffer.wait_for_item t.low_prio_deletes;
        Lwt_buffer.wait_for_item t.rocksdb_sync;
      ] >>= fun () ->

    _perform_reads_from_buffer t.high_prio_reads 10_000_000 >>= fun _ ->
    _perform_reads_from_buffer t.low_prio_reads   2_000_000 >>= fun _ ->

    begin
      (* batch up writes *)

      let rec harvest buf acc cost threshold =
        if cost >= threshold
        then acc, cost
        else
          match Lwt_buffer.take_no_wait buf with
          | None -> acc, cost
          | Some (blobs, waker) ->
             let cost' =
               List.fold_left
                 (fun cost (fnr, blob) ->
                  cost + Asd_protocol.Blob.length blob)
                 cost
                 blobs
             in
             harvest
               buf
               ((blobs, waker) :: acc)
               cost'
               threshold
      in

      let write_batch_cost = 10_000_000 in
      let write_batch_reserved_for_high_prio = 7_000_000 in

      (* first fill reserved slot with high prio writes *)
      let acc, cost =
        harvest
          t.high_prio_writes
          [] 0 write_batch_reserved_for_high_prio
      in
      (* next fill up the rest with low prio writes *)
      let acc, cost =
        harvest
          t.low_prio_writes
          acc cost write_batch_cost
      in
      (* finally if the batch is not yet full add some
       * more high prio writes *)
      let acc, cost =
        harvest
          t.high_prio_writes
          acc cost write_batch_cost
      in

      let waiters_len = List.length acc in

      if waiters_len > 0
      then
        begin
          with_timing_lwt
            (fun () ->

             let blobs_count =
               List.fold_left
                 (fun acc (blobs, _) ->
                  acc + List.length blobs)
                 0
                 acc
             in

             let module FL = Lwt_extra2.FoldingCountDownLatch in
             let latch = FL.create
                           ~count:blobs_count
                           ~acc:[]
                           ~f:(fun acc parent_dir ->
                               if List.mem parent_dir acc
                               then acc
                               else parent_dir :: acc)
             in

             let sync_parent_dirs_t =
               if fsync
               then
                 begin
                   FL.await latch >>= fun parent_dirs ->
                   Lwt_log.debug_f
                     "Syncing parent dirs %s"
                     ([%show : string list] parent_dirs) >>= fun () ->
                   Lwt_list.iter_p
                     (fun parent_dir ->
                      Lwt_extra2.fsync_dir parent_dir)
                     parent_dirs
                 end
               else
                 Lwt.return ()
             in

             Lwt_list.map_p
               (fun (blobs, waker) ->
                Lwt_list.iter_p
                  (fun (fnr, blob) ->
                   t.write_blob
                     fnr blob
                     (fun fd len parent_dir ->

                      if fsync
                      then
                        begin
                          (* wait for all the other blobs to be pushed to
                           * the filesystem too before syncing *)
                          FL.notify latch parent_dir;
                          FL.await latch >>= fun _ ->

                          Lwt_unix.fsync fd
                        end
                      else
                        Lwt.return_unit))
                  blobs >>= fun () ->
                Lwt.return waker)
               acc >>= fun wakers ->

             sync_parent_dirs_t >>= fun () ->

             List.iter
               (fun (_, waker) ->
                Lwt.wakeup_later waker ())
               acc;

             Lwt.return_unit) >>= fun (t_fsyncs, ()) ->

          let log_level =
            if t_fsyncs < 0.5 then Lwt_log.Debug
            else if t_fsyncs < 4.0 then Lwt_log.Info
            else Lwt_log.Warning
          in
          if log_level >= (Lwt_log.Section.level Lwt_log.Section.main)
          then
            Lwt_log.log_f
              ~level:log_level
              "writing blobs + syncing(%b) took %f for %i waiters, cost %i"
              fsync t_fsyncs waiters_len cost
          else
            Lwt.return_unit
        end
      else
        Lwt.return_unit
    end >>= fun () ->

    _perform_deletes_from_buffer t.high_prio_deletes 10 >>= fun () ->
    _perform_deletes_from_buffer t.low_prio_deletes 5 >>= fun () ->

    (if Lwt_buffer.has_item t.rocksdb_sync
     then
       begin
         Lwt_buffer.harvest t.rocksdb_sync >>= fun wakers ->
         t.sync_rocksdb ();
         List.iter
           (fun waker -> Lwt.wakeup_later waker ())
           wakers;
         Lwt.return_unit
       end
     else
       Lwt.return_unit) >>= fun () ->

    inner ()
  in
  inner ()
