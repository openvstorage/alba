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

open Ctypes
open Foreign

let reed_sol_vandermonde_coding_matrix =
  let inner =
    (* int *reed_sol_vandermonde_coding_matrix(int k, int m, int w); *)
    foreign
      "reed_sol_vandermonde_coding_matrix"
      (int @-> int @-> int @-> returning (ptr int))
  in
  fun ~k ~m ~w ->
  inner k m w

let reed_sol_big_vandermonde_distribution_matrix =
  let inner =
    (* int *reed_sol_big_vandermonde_distribution_matrix(int rows, int cols, int w); *)
    foreign
      "reed_sol_big_vandermonde_distribution_matrix"
      (int @-> int @-> int @-> returning (ptr int))
  in
  fun ~rows ~cols ~w ->
  inner rows cols w

let _jerasure_matrix_encode ?release_runtime_lock =
  foreign
    "jerasure_matrix_encode"
    ?release_runtime_lock
    (int @-> int @-> int @-> ptr int @->
     ptr (ptr char) @-> ptr (ptr char) @->
     int @-> returning void)

let jerasure_matrix_encode k m w matrix data_ptrs coding_ptrs size =
  Lwt_preemptive.detach
    (fun () ->
       _jerasure_matrix_encode
         ~release_runtime_lock:true
         k m w
         matrix
         data_ptrs coding_ptrs
         size)
    ()

let _jerasure_matrix_decode ?release_runtime_lock =
  foreign
    "jerasure_matrix_decode"
    ?release_runtime_lock
    (int @-> int @-> int @-> ptr int @->
     int @-> ptr int @->
     ptr (ptr char) @-> ptr (ptr char) @->
     int @-> returning void)

let jerasure_matrix_decode ~k ~m ~w matrix row_k_ones erasures data_ptrs coding_ptrs size =
  Lwt_preemptive.detach
    (fun () ->
       _jerasure_matrix_decode
         ~release_runtime_lock:true
         k m w
         matrix row_k_ones erasures
         data_ptrs coding_ptrs
         size)
    ()

let jerasure_erasures_to_erased =
  let inner =
    (* int *jerasure_erasures_to_erased(int k, int m, int *erasures) *)
    foreign
      "jerasure_erasures_to_erased"
      (int @-> int @-> ptr int @-> returning (ptr int))
  in
  fun ~k ~m ~erasures ->
  inner k m (CArray.start (CArray.of_list int erasures))

let jerasure_make_decoding_matrix =
  let inner =
    (* jerasure_make_decoding_matrix/bitmatrix make the k*k decoding matrix *)
    (*       (or wk*wk bitmatrix) by taking the rows corresponding to k *)
    (*       non-erased devices of the distribution matrix, and then *)
    (*       inverting that matrix. *)

    (*       You should already have allocated the decoding matrix and *)
    (*       dm_ids, which is a vector of k integers.  These will be *)
    (*       filled in appropriately.  dm_ids[i] is the id of element *)
    (*       i of the survivors vector.  I.e. row i of the decoding matrix *)
    (*       times dm_ids equals data drive i. *)

    (*       Both of these routines take "erased" instead of "erasures". *)
    (*       Erased is a vector with k+m elements, which has 0 or 1 for  *)
    (*       each device's id, according to whether the device is erased. *)

    (* int jerasure_make_decoding_matrix(int k, int m, int w, int *matrix, int *erased,  *)
    (*                                 int *decoding_matrix, int *dm_ids); *)
    foreign
      "jerasure_make_decoding_matrix"
      (int @-> int @-> int @-> ptr int @-> ptr int @-> ptr int @-> ptr int @-> returning int)
  in
  fun ~k ~m ~w ~cm ~erased ->
  let decoding_matrix = allocate_n int ~count:(k*k) in
  let dm_ids = allocate_n int ~count:k in
  let res =
    inner
      k m w
      cm
      erased
      decoding_matrix dm_ids
  in
  assert (res = 0);
  decoding_matrix
