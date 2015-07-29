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

open Asd_protocol
open Lwt

let progress t0 step i =
  let row = 10 * step in
  if i mod step = 0 && i <> 0
  then
    Lwt_io.printf "%16i" i >>= fun () ->
    if (i mod row = 0 )
    then
      let t1 = Unix.gettimeofday() in
      let dt = t1 -. t0 in
      let speed = (float i) /. dt in
      Lwt_io.printlf " (%8.2fs;%6.2f/s)" dt speed
    else Lwt.return ()
  else Lwt.return ()


let measured_loop f n =
  let t0 = Unix.gettimeofday () in
  let step = n / 100 in
  let rec loop min_d max_d i =
    if i = n
    then Lwt.return (min_d, max_d)
    else
      progress t0 step i >>= fun () ->
      let t1 = Unix.gettimeofday() in
      f i >>= fun () ->
      let t2 = Unix.gettimeofday() in
      let d = t2 -. t1 in
      let min_d' = min d min_d
      and max_d' = max d max_d
      in
      loop min_d' max_d' (i+1)
  in
  loop max_float 0. 0 >>= fun (min_d, max_d) ->
  let t1 = Unix.gettimeofday () in
  let d = t1 -. t0 in
  let nf = float n in
  let speed = nf /. d in
  let latency = d /. nf in
  Lwt.return (d,speed, latency, min_d, max_d)

let report name (d,speed, latency, min_d, max_d) =
  Lwt_io.printlf "\n%s" name >>= fun () ->
  Lwt_io.printlf "\ttook: %fs or (%f /s)" d speed >>= fun () ->
  Lwt_io.printlf "\tlatency: %fms" (latency *. 1000.0) >>= fun () ->
  Lwt_io.printlf "\tmin: %fms" (min_d *. 1000.0) >>= fun () ->
  Lwt_io.printlf "\tmax: %fms" (max_d *. 1000.0)

let _make_key period prefix =
  (* math:
     X_{n+1} = (aX_{n} + c) mod m
     if a = 21, c = 3 and m = 10^ x
     then this one is of full period
    *)
  let x = ref 42 in
  fun () ->
    let i = !x in
    let x' = (21 * !x + 3) mod period in
    let () = x := x'  in
    Printf.sprintf
      "%s%s_%016i"
      Osd_keys.bench_prefix prefix i

let make_key power prefix =
  let period =
    let rec loop acc = function
      | 0 -> acc
      | i -> loop (10 * acc) (i-1)
    in
    loop 1 power
  in
  _make_key period prefix

let deletes (client: Osd.osd) n value_size power prefix =
  let gen = make_key power prefix in
  let do_one i =
    let key = gen () in
    let open Slice in
    let key_slice = Slice.wrap_string key in
    let delete = Update.Set (key_slice, None) in
    let updates = [delete] in
    client # apply_sequence [] updates >>= fun _result ->
    Lwt.return ()
  in
  measured_loop do_one n >>= fun r ->
  report "deletes" r


let gets (client: Osd.osd) n value_size power prefix =
  let gen = make_key power prefix in
  let do_one i =
    let key = gen () in
    let open Slice in
    let key_slice = Slice.wrap_string key in
    client # get_option key_slice >>= fun _value ->
    let () =
      match _value with
      | None   -> failwith (Printf.sprintf "db[%s] = None?" key)
      | Some v -> assert (v.Slice.length = value_size)
    in
    Lwt.return ()
  in
  measured_loop do_one n >>= fun r ->
  report "gets" r

let sets (client:Osd.osd) n value_size power prefix =
  let gen = make_key power prefix in
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
    let value_slice = Slice.wrap_string value in
    let open Checksum in
    let set = Update.Set (key_slice,
                          Some (value_slice, Checksum.NoChecksum, false))

    in
    let updates = [set] in
    client # apply_sequence [] updates >>= fun _result ->
    Lwt.return ()
  in
  measured_loop do_one n >>= fun r ->
  report "sets" r


let do_all client n value_size power prefix=
  let scenario = [
      sets;
      gets;
      deletes;
    ]
  in
  Lwt_list.iter_s
    (fun which ->
     which client
           n value_size power prefix
    )
    scenario
