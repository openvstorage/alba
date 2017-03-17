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
open Lwt.Infix
open Ctypes_helper
open Lwt_bytes2

let test_free_cm () =
  let k, m, w = 2, 3, 8 in
  for _ = 0 to 1_000_000 do
    with_free
      (fun () -> Jerasure.reed_sol_vandermonde_coding_matrix ~k ~m ~w)
      ignore
  done

let make_parity len m  =
  List.map
    (fun _ -> SharedBuffer.create len)
    (Int.range 0 m)

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
      [ SharedBuffer.of_string data1;
        SharedBuffer.of_string data2;
        SharedBuffer.of_string data3;
        SharedBuffer.of_string data4;
      ] in
  let t () =
    let parity = make_parity len m  in
    Erasure.encode'
      ~kind:Erasure.Isa_l
      ~k ~m ~w
      data parity
      len >>= fun () ->
    Printf.printf
      "parity'=%s\n"
      ([%show : string list] (List.map SharedBuffer.to_string parity));

    let parity' = make_parity len m in

    Erasure.encode'
      ~kind:Erasure.Jerasure
      ~k ~m ~w
      data parity'
      len >>= fun () ->

    Printf.printf
      "parity'=%s\n"
      ([%show : string list] (List.map SharedBuffer.to_string parity'));

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
      [ SharedBuffer.of_string data1;
        SharedBuffer.of_string data2;
        SharedBuffer.of_string data3;
        SharedBuffer.of_string data4;
      ] in
    let parity = make_parity len m in
    Erasure.encode'
      ~kind
      ~k ~m ~w
      data parity
      len >>= fun () ->
    Printf.printf
      "parity=%s\n"
      ([%show : string list] (List.map SharedBuffer.to_string parity));

    let erasures = [0; k; -1] in

    let data' = SharedBuffer.create len :: List.tl_exn data in
    let parity' = SharedBuffer.create len :: List.tl_exn parity in

    Erasure.decode
      ~kind
      ~k ~m ~w
      erasures
      data' parity'
      len >>= fun () ->

    Printf.printf
      "data'=%s\n"
      ([%show : string list] (List.map SharedBuffer.to_string data'));
    Printf.printf
      "parity'=%s\n"
      ([%show : string list] (List.map SharedBuffer.to_string parity'));

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
