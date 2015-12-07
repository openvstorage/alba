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
open Asd_protocol
open Lwt
open Generic_bench

let maybe_fail = function
  | Osd.Ok -> Lwt.return_unit
  | Osd.Exn e -> Osd.Error.lwt_fail e

let deletes (client: Osd.osd) progress n value_size period prefix =
  let gen = make_key period prefix in
  let do_one i =
    let key = gen () in
    let open Slice in
    let key_slice = Slice.wrap_string key in
    let delete = Update.Set (key_slice, None) in
    let updates = [delete] in
    client # apply_sequence Osd.High [] updates >>= maybe_fail
  in
  measured_loop progress do_one n >>= fun r ->
  report "deletes" r


let gets (client: Osd.osd) progress n value_size period prefix =
  let gen = make_key period prefix in
  let do_one i =
    let key = gen () in
    let open Slice in
    let key_slice = Slice.wrap_string key in
    client # get_option Osd.High key_slice >>= fun _value ->
    let () =
      match _value with
      | None   -> failwith (Printf.sprintf "db[%s] = None?" key)
      | Some v -> assert (Lwt_bytes.length v = value_size)
    in
    Lwt.return ()
  in
  measured_loop progress do_one n >>= fun r ->
  report "gets" r

let sets (client:Osd.osd) progress n value_size period prefix =
  let gen = make_key period prefix in
  (* TODO: this affects performance as there is compression going
     on inside the database
   *)
  let open Slice in
  let value = Bytes.init
                value_size
                (fun i ->
                 let t0 = i+1 in
                 let t1 = t0 * t0 -1 in
                 let t2 = t1 mod 65535 in
                 let t3 = t2 mod 251 in
                 Char.chr t3)

  in
  let do_one i =
    let key = gen () in
    let key_slice = Slice.wrap_string key in
    let open Checksum in
    let set = Update.Set (key_slice,
                          Some (Blob.Bytes value,
                                Checksum.NoChecksum,
                                false))

    in
    let updates = [set] in
    client # apply_sequence Osd.High [] updates >>= maybe_fail
  in
  measured_loop progress do_one n >>= fun r ->
  report "sets" r

let do_scenarios
      with_client
      n_clients n
      value_size power prefix
      scenarios
  =
  let period = period_of_power power in
  Lwt_list.iter_s
    (fun scenario ->
     let progress = make_progress (n/100) in
     Lwt_list.iter_p
       (fun i ->
        with_client
          (fun client ->
           scenario
             client
             progress
             (n/n_clients)
             value_size
             period
             (Printf.sprintf "%s_%i" prefix i)))
       (Int.range 0 n_clients))
    scenarios
