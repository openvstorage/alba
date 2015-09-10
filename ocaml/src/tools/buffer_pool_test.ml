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
