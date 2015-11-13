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

open Prelude
open Lwt_buffer
open Lwt.Infix

(* performs some IO and returns how costly it was *)
type 'a io = unit -> ('a * int) Lwt.t

type 'a io_with_waiter = 'a Lwt.u * 'a io
type output = unit  io_with_waiter
type input  = (Llio2.WriteBuffer.t * (Lwt_unix.file_descr -> unit Lwt.t)) io_with_waiter

type t = {
    high_prio_reads  : input  Lwt_buffer.t;
    high_prio_writes : output Lwt_buffer.t;
    low_prio_reads   : input  Lwt_buffer.t;
    low_prio_writes  : output Lwt_buffer.t;
  }

let make () =
  { high_prio_reads  = Lwt_buffer.create ();
    high_prio_writes = Lwt_buffer.create ();
    low_prio_reads   = Lwt_buffer.create ();
    low_prio_writes  = Lwt_buffer.create (); }

type priority =
  | High
  | Low

let perform_read t prio r =
  let sleep, waker = Lwt.wait () in
  Lwt_buffer.add
    (waker, r)
    (match prio with
     | High -> t.high_prio_reads
     | Low  -> t.low_prio_reads) >>= fun () ->
  sleep

let perform_write t prio w =
  let sleep, waker = Lwt.wait () in
  Lwt_buffer.add
    (waker, w)
    (match prio with
     | High -> t.high_prio_writes
     | Low  -> t.low_prio_writes) >>= fun () ->
  sleep

let rec _harvest_from_buffer buffer acc cost threshold =
  if cost >= threshold || not (Lwt_buffer.has_item buffer)
  then Lwt.return (acc, cost)
  else
    begin
      Lwt_buffer.take buffer >>= fun (waiter, item) ->
      Lwt.catch
        (fun () ->
         item () >>= fun (res, cost) ->
         Lwt.return (`Ok res, cost))
        (fun exn ->
         Lwt.return (`Exn exn, 1000)) >>= fun (res, cost') ->
      _harvest_from_buffer
        buffer
        ((waiter, res) :: acc)
        (cost + cost')
        threshold
    end

let _notify_waiters res =
  List.iter
    (function
      | (waker, `Ok res) -> Lwt.wakeup_later waker res
      | (waker, `Exn exn) -> Lwt.wakeup_later_exn waker exn)
    res

let _perform_reads_from_buffer buffer threshold =
  _harvest_from_buffer buffer [] 0 threshold >>= fun (results, _) ->
  _notify_waiters results;
  Lwt.return_unit

let run t ~fsync ~fs_fd =
  let rec inner () =
    Lwt.pick
      [ Lwt_buffer.wait_for_item t.high_prio_writes;
        Lwt_buffer.wait_for_item t.high_prio_reads;
        Lwt_buffer.wait_for_item t.low_prio_writes;
        Lwt_buffer.wait_for_item t.low_prio_reads;
      ] >>= fun () ->

    _perform_reads_from_buffer t.high_prio_reads 10_000_000 >>= fun _ ->
    _perform_reads_from_buffer t.low_prio_reads   2_000_000 >>= fun _ ->

    begin
      (* batch up writes *)
      let write_batch_cost = 10_000_000 in
      let write_batch_reserved_for_high_prio = 7_000_000 in

      (* first fill reserved slot with high prio writes *)
      _harvest_from_buffer
        t.high_prio_writes
        [] 0 write_batch_reserved_for_high_prio
      >>= fun (res, cost) ->

      (* next fill up the rest with low prio writes *)
      _harvest_from_buffer
        t.low_prio_writes
        res cost write_batch_cost
      >>= fun (res, cost) ->

      (* finally if the batch is not yet full add some
       * more high prio writes *)
      _harvest_from_buffer
        t.high_prio_writes
        res cost write_batch_cost
      >>= fun (res, cost) ->

      (if fsync
          (* no need to sync if there are no waiters! *)
          && res <> []
       then begin
           let waiters_len = List.length res in
           Lwt_log.debug_f "Starting syncfs for %i waiters" waiters_len >>= fun () ->
           with_timing_lwt
             (fun () -> Syncfs.lwt_syncfs fs_fd) >>= fun (t_syncfs, rc) ->
           assert (rc = 0);
           let logger =
             if t_syncfs < 0.5 then Lwt_log.debug_f
             else if t_syncfs < 4.0 then Lwt_log.info_f
             else Lwt_log.warning_f
           in
           logger "syncfs took %f for %i waiters, cost %i" t_syncfs waiters_len cost
         end else
         Lwt.return_unit)
      >>= fun () ->
      _notify_waiters res;
      Lwt.return_unit
    end >>= fun () ->

    inner ()
  in
  inner ()
