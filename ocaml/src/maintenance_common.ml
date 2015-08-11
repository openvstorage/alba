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

let maintenance_for_all_x task_name list_x maintenance_f get_x_id show_x =
  let x_threads = Hashtbl.create 4 in

  let sync_x_threads () =
    list_x () >>= fun (_, xs) ->

    List.iter
      (fun x ->
         let x_id = get_x_id x in
         if Hashtbl.mem x_threads x_id
         then ()
         else begin
           let t =
             Lwt.catch
               (fun () ->
                  Lwt_log.debug_f
                    "Starting %s for %s"
                    task_name (show_x x) >>= fun () ->
                  maintenance_f x)
               (fun exn ->
                  Lwt_log.debug_f
                    ~exn
                    "Thread %s for %s stopped due to an exception"
                    task_name (show_x x))
             >>= fun () ->
             Hashtbl.remove x_threads x_id;
             Lwt.return ()
           in
           Lwt.ignore_result t;
           Hashtbl.add x_threads x_id ()
         end)
      xs;

    Lwt.return ()
  in
  Lwt_extra2.run_forever
    (Printf.sprintf "Got unexpected exception in main %s thread" task_name)
    sync_x_threads
    60.
