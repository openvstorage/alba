(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

open Prelude
open Lwt.Infix
open Ctypes_helper

let test_free_cm () =
  let k, m, w = 2, 3, 8 in
  for _ = 0 to 1_000_000 do
    with_free
      (fun () -> Jerasure.reed_sol_vandermonde_coding_matrix ~k ~m ~w)
      ignore
  done

let test_isa_l_jerasure () =
  let open Lwt.Infix in
  let k = 4 in
  let m = 13 in
  let w = 8 in
  let len = 128 in
  let data1 = String.make len 'a' in
  let data2 = String.make len 'b' in
  let data3 = String.make len 'c' in
  let data4 = String.make len 'd' in
  let data =
      [ Lwt_bytes.of_string data1;
        Lwt_bytes.of_string data2;
        Lwt_bytes.of_string data3;
        Lwt_bytes.of_string data4;
      ] in
  let t () =
    let parity =
      List.map
        (fun _ -> Lwt_bytes.create len)
        (Int.range 0 m)
    in
    Erasure.encode'
      ~kind:Erasure.Isa_l
      ~k ~m ~w
      data parity
      len >>= fun () ->
    Printf.printf
      "parity'=%s\n"
      ([%show : string list] (List.map Lwt_bytes.to_string parity));

    let parity' =
      List.map
        (fun _ -> Lwt_bytes.create len)
        (Int.range 0 m)
    in
    Erasure.encode'
      ~kind:Erasure.Jerasure
      ~k ~m ~w
      data parity'
      len >>= fun () ->

    Printf.printf
      "parity'=%s\n"
      ([%show : string list] (List.map Lwt_bytes.to_string parity'));

    assert (parity = parity');

    Lwt.return ()
  in
  Lwt_main.run (t ())

let test_encode_decode () =
  let t kind =
    let k = 4 in
    let m = 13 in
    let w = 8 in
    let len = 128 in
    let data1 = String.make len 'a' in
    let data2 = String.make len 'b' in
    let data3 = String.make len 'c' in
    let data4 = String.make len 'd' in
    let data =
      [ Lwt_bytes.of_string data1;
        Lwt_bytes.of_string data2;
        Lwt_bytes.of_string data3;
        Lwt_bytes.of_string data4;
      ] in
    let parity =
      List.map
        (fun _ -> Lwt_bytes.create len)
        (Int.range 0 m)
    in
    Erasure.encode'
      ~kind
      ~k ~m ~w
      data parity
      len >>= fun () ->
    Printf.printf
      "parity=%s\n"
      ([%show : string list] (List.map Lwt_bytes.to_string parity));

    let erasures = [0; k; -1] in

    let data' = Lwt_bytes.create len :: List.tl_exn data in
    let parity' = Lwt_bytes.create len :: List.tl_exn parity in

    Erasure.decode
      ~kind
      ~k ~m ~w
      erasures 
      data' parity'
      len >>= fun () ->

    Printf.printf
      "data'=%s\n"
      ([%show : string list] (List.map Lwt_bytes.to_string data'));
    Printf.printf
      "parity'=%s\n"
      ([%show : string list] (List.map Lwt_bytes.to_string parity'));

    assert (data' = data);
    assert (parity' = parity);

    Lwt.return ()
  in
  Lwt_main.run
    (t Erasure.Jerasure >>= fun () ->
     t Erasure.Isa_l)

open OUnit

let suite = "erasure" >:::[
    "test_isa_l_jerasure" >:: test_isa_l_jerasure;
    "test_free_cm" >:: test_free_cm;
    "test_encode_decode" >:: test_encode_decode;
]
