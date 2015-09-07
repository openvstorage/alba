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
