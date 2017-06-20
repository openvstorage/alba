(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Lwt.Infix

type check_result =
  | Keep
  | DropThis
  | DropPool

type 'a t = {
  factory : unit -> 'a Lwt.t;
  cleanup : 'a -> unit Lwt.t;
  check : 'a -> exn -> check_result;
  max_size : int;
  mutable count : int;
  available_items : 'a Queue.t;
  waiters : unit Lwt.u Queue.t;
  mutable finalizing : bool;
}

let create max_size ~check ~factory ~cleanup =
  { factory; cleanup;
    check; max_size;
    count = 0; available_items = Queue.create ();
    waiters = Queue.create ();
    finalizing = false;
  }

let rec acquire t =
  if Queue.is_empty t.available_items
  then begin
    if t.count < t.max_size
    then begin
      (* create one extra *)
      t.count <- t.count + 1;
      Lwt.catch
        (fun () -> t.factory ())
        (fun exn ->
           t.count <- t.count - 1;
           Lwt.fail exn)
    end else begin
      (* wait for a member to become available *)
      let t', u = Lwt.wait () in
      Queue.push u t.waiters;
      t' >>= fun () ->
      acquire t
    end
  end else
    (* take an available member *)
    let a = Queue.pop t.available_items in
    Lwt.return a

let use t f =
  if t.finalizing
  then Lwt.fail_with "attempting to use a resource from a pool which is being finalized"
  else begin
    Lwt.finalize
      (fun () ->
         acquire t >>= fun a ->
         Lwt.catch
           (fun () ->
              f a >>= fun r ->
              (if t.finalizing
               then begin
                 t.count <- t.count - 1;
                 t.cleanup a
               end else begin
                 Queue.push a t.available_items;
                 Lwt.return ()
               end) >>= fun () ->
              Lwt.return r)
           (fun exn ->
              match t.check a exn with
              | Keep ->
                begin
                  Queue.push a t.available_items;
                  Lwt.fail exn
                end
              | DropThis ->
                begin
                  let () =
                    t.count <- t.count - 1;
                    Lwt.async (fun () -> t.cleanup a)
                  in
                  Lwt.fail exn
                end
              | DropPool ->
                 begin
                   let () =
                     t.count <- 0;
                     let rec pop_all acc =
                       if Queue.is_empty t.available_items
                       then acc
                       else
                         let a = Queue.pop t.available_items in
                         pop_all (a::acc)
                     in
                     let items = pop_all [] in
                     Lwt.async
                       (fun () ->
                         Lwt_list.iter_p
                           (fun a -> t.cleanup a)
                           (a :: items)
                       )
                   in
                   Lwt.fail exn
                 end

      ))
      (fun () ->
         (* wake up the first waiter *)
         (try Lwt.wakeup (Queue.pop t.waiters) ()
          with Queue.Empty -> ());
         Lwt.return ())
  end

let finalize t =
  t.finalizing <- true;
  let ts =
    Queue.fold
      (fun acc item ->
         t.cleanup item :: acc)
      []
      t.available_items in
  Lwt.join ts
