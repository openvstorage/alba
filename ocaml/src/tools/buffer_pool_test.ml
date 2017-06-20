(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Buffer_pool

let test_buffer_pool () =
  let pool = create ~buffer_size:1 in
  let a = get_buffer pool in
  let b = get_buffer pool in
  return_buffer pool a;
  (* setting a bad example here by returning the same buffer
     to this pool multiple times *)
  return_buffer pool b;
  return_buffer pool b;
  return_buffer pool b;
  (* physical equality *)
  assert (a == get_buffer pool);
  assert (b == get_buffer pool);
  assert (b == get_buffer pool);
  assert (b == get_buffer pool);
  let c = get_buffer pool in
  assert (a != b);
  assert (a != c);
  assert (b != c)

open OUnit

let suite = "buffer_pool_test" >:::[
    "test_buffer_pool" >:: test_buffer_pool
  ]
