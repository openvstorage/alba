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
open Nsm_model
open Key_value_store
open Mem_key_value_store


module KV = Mem_key_value_store
module NSMA = NamespaceManager(struct let namespace_id = 3L end)(KV)

let assert_failswith err f =
  try
    f ();
    failwith "function did not fail as expected"
  with Err.Nsm_exn (err', _) when err' = err -> ()

let ok_or_die = function
  | Ok -> ()
  | x -> failwith ([%show:status] x)

let test_gc_epoch () =
  let kv = Mem_key_value_store.create () in
  let open GcEpochs in
  let _, { minimum_epoch; next_epoch } = NSMA.get_gc_epochs kv in
  assert (minimum_epoch = 0L);
  assert (next_epoch = 1L);
  ok_or_die (KV.apply_sequence kv
                               (NSMA.enable_new_gc_epoch kv 1L));

  let _, {minimum_epoch; next_epoch} = NSMA.get_gc_epochs kv in
  assert (minimum_epoch = 0L);
  assert (next_epoch = 2L);
  ok_or_die (KV.apply_sequence kv (NSMA.disable_gc_epoch kv 0L));
  let _, {minimum_epoch; next_epoch} = NSMA.get_gc_epochs kv in
  assert (minimum_epoch = 1L);
  assert (next_epoch = 2L)


open OUnit

let suite = "namespace_manager_model" >:::[
    "test_gc_epoch" >:: test_gc_epoch;
  ]
