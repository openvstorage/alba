(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
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
