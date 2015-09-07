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

open OUnit

let doubles test_ctxt =
 (* this verifies that
       - no command is specified twice
       - no tag is used twice *)
    let h1 = Hashtbl.create 3 in
    let h2 = Hashtbl.create 3 in
    List.iter
      (fun (c, t, _) ->
         assert_bool "not mem h1" (not (Hashtbl.mem h1 c));
         Hashtbl.add h1 c ();
         assert_bool "not mem h2"(not (Hashtbl.mem h2 t));
         Hashtbl.add h2 t ();
         ())
      Nsm_host_protocol.Protocol.command_map


let suite = ["doubles" >:: doubles]
