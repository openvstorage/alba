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
