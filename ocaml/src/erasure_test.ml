(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
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

let show_fragments p =
  List.map SharedBuffer.to_string p |> [%show : string list]

let cmp_fragments a b = show_fragments a = show_fragments b

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
    Lwt_io.printlf "parity'=%s" (show_fragments parity) >>= fun () ->

    let parity' = make_parity len m in

    Erasure.encode'
      ~kind:Erasure.Jerasure
      ~k ~m ~w
      data parity'
      len >>= fun () ->

    Lwt_io.printlf "parity'=%s%!" (show_fragments parity') >>= fun () ->
    let () =
      Prelude.finalize
        (fun () -> OUnit.assert_equal
                     parity parity'
                     ~printer:show_fragments
                     ~cmp:cmp_fragments
        )
        (fun () ->
          List.iter SharedBuffer.unregister_usage data;
          List.iter SharedBuffer.unregister_usage parity';
          List.iter SharedBuffer.unregister_usage parity;
        )
    in
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
    Lwt_io.printlf "parity=%s" (show_fragments parity) >>= fun () ->

    let erasures = [0; k; -1] in
    let d0 = SharedBuffer.create len in
    let data' = d0 :: List.tl_exn data in
    let p0 = SharedBuffer.create len in
    let parity' = p0 :: List.tl_exn parity in

    Erasure.decode
      ~kind
      ~k ~m ~w
      erasures
      data' parity'
      len >>= fun () ->

    Lwt_io.printlf "data'=%s"     (show_fragments data') >>= fun () ->
    Lwt_io.printlf "parity'=%s%!" (show_fragments parity') >>= fun ()->

    let () =
      Prelude.finalize
        (fun () ->
          OUnit.assert_equal data' data
                             ~printer:show_fragments
                             ~cmp:cmp_fragments;
          OUnit.assert_equal parity' parity
                             ~printer:show_fragments
                             ~cmp:cmp_fragments)
        (fun () ->
          List.iter SharedBuffer.unregister_usage data;
          List.iter SharedBuffer.unregister_usage parity;
          List.iter SharedBuffer.unregister_usage [d0;p0];
        )
    in
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
