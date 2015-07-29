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

type 'a t = {
  factory : unit -> 'a Lwt.t;
  cleanup : 'a -> unit Lwt.t;
  check : 'a -> exn -> bool;
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
              if t.check a exn
              then Lwt.fail exn
              else begin
                t.count <- t.count - 1;
                t.cleanup a >>= fun () ->
                Lwt.fail exn
              end))
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
  Lwt.ignore_result (Lwt.join ts)
