(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Fragment_size_helper

let test_determine_chunk_size () =
  let v = [
      (* fragment_size, object_length, k, expected chunk_size *)
      160, 1600, 2, 320;
      160, 320, 2, 320;
      160, 319, 2, 320;
      160, 305, 2, 320;
      160, 304, 2, 320;
      160, 289, 2, 320;
      160, 288, 2, 288;
      160, 165, 2, 160+32;
      160, 100, 2, ((100/32)+1)*32;
      160, 32, 2, 32;
      160, 16, 2, 32;
      160,  1, 2, 32;
      160,  0, 2,  0;
    ] in
  List.iter
    (fun (max_fragment_size, object_length, k, expected_chunk_size) ->
      let chunk_size = determine_chunk_size ~object_length
                                            ~max_fragment_size
                                            ~k
      in
      assert (chunk_size = expected_chunk_size);
      let old_chunk_size = old_desired_chunk_size ~object_length
                                                  ~max_fragment_size
                                                  ~k
      in
      Printf.printf "%i, %i, %i, %i, %i\n"
                    max_fragment_size object_length k
                    chunk_size
                    old_chunk_size)
    v

open OUnit

let suite = "fragment_size_helper_test" >:::[
      "test_determine_chunk_size" >:: test_determine_chunk_size;
    ]
