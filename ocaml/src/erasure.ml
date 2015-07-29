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

open Prelude
open Ctypes
open Lwt.Infix

let pointers_of fragments =
  CArray.of_list
    (ptr char)
    (List.map (bigarray_start array1) fragments)

let get_start_ptr fragments = CArray.start (pointers_of fragments)

let list_to_carray (l : Lwt_bytes.t list) =
  let open CArray in
  start
    (of_list
       (ptr char)
       (List.map (bigarray_start array1) l))

let with_free = Ctypes_helper.with_free
let with_free_lwt = Ctypes_helper.with_free_lwt

type kind =
  | Jerasure
  | Isa_l

let encode ?(kind=Isa_l) ~k ~m ~w data parity size =
  let data_ptrs = list_to_carray data in
  let parity_ptrs = list_to_carray parity in
  match kind with
  | Jerasure ->
    with_free_lwt
      (fun () -> Jerasure.reed_sol_vandermonde_coding_matrix ~k ~m ~w)
      (fun cm ->
       Jerasure.jerasure_matrix_encode
         k m w
         cm
         data_ptrs parity_ptrs
         size)
  | Isa_l ->
    let rsm =
      Isa_l.gen_rs_vandermonde_matrix_jerasure ~k ~m
      |> bigarray_start array1
    in
    let gftbls =
      Isa_l.init_tables
        ~k ~rows:(k + m)
        (rsm
         |> to_voidp |> from_voidp uchar
         |> fun x -> x +@ k*k) in
    Lwt_preemptive.detach
      (fun () ->
       Isa_l.encode_data
         ~release_runtime_lock:true
         ~len:size
         ~k ~rows:m
         ~gftbls
         ~data_in:data_ptrs
         ~data_out:parity_ptrs)
      ()


let decode ?(kind=Isa_l) ~k ~m ~w erasures data parity size =
  if erasures <> [ -1 ]
  then begin
    match kind with
    | Jerasure ->
       with_free_lwt
         (fun () -> Jerasure.reed_sol_vandermonde_coding_matrix ~k ~m ~w)
         (fun cm ->
          let row_k_ones = 1 in
          Jerasure.jerasure_matrix_decode
            ~k ~m ~w
            cm row_k_ones
            (Ctypes.CArray.start (Ctypes.CArray.of_list Ctypes.int erasures))
            (list_to_carray data) (list_to_carray parity)
            size)
    | Isa_l ->
       (* TODO this decode is rather suboptimal. It will
          first decode all data fragments (even those that
          are already there!), and then reencode
          these to regenerate all the parity fragments
          (that step is needed because some code currently
          assumes this will be done when decoding...).
          So for now too much decoding/copying happens.
          Should be optimized someday.
        *)

       let dm, erased =
         with_free
           (fun () -> Jerasure.reed_sol_vandermonde_coding_matrix ~k ~m ~w)
           (fun cm ->
            with_free
              (fun () -> Jerasure.jerasure_erasures_to_erased ~k ~m ~erasures)
              (fun erased ->
               let res =
                 Jerasure.jerasure_make_decoding_matrix
                   ~k ~m ~w
                   ~cm
                   ~erased in
               let erased' =
                 let open CArray in
                 from_ptr erased (k+m)
                 |> to_list
                 |> List.map (fun i -> i > 0)
               in
               (res, erased')))
       in

       let rsm = Ctypes.(bigarray_start
                           array1
                           (Isa_l.rs_vandermonde_matrix_from_jerasure ~k ~rows:k dm)) in

       let all_fragments = List.append data parity in

       let _, data_in =
         List.fold_left
           (fun (i, acc_in) erased ->
            let fragment = List.nth_exn all_fragments i in
            let i' = i + 1 in
            if erased
            then i', acc_in
            else i', fragment::acc_in)
           (0, [])
           erased
       in
       let data_in = List.rev data_in in
       assert (k <= List.length data_in);
       let data_out = data in
       let gftbls =
         let open Ctypes in
         Isa_l.init_tables
           ~k ~rows:k
           (rsm
            |> to_voidp |> from_voidp uchar
            |> fun x -> x) in

       Lwt_preemptive.detach
         (fun () ->
          Isa_l.encode_data
            ~release_runtime_lock:true
            ~len:size
            ~k ~rows:k
            ~gftbls
            ~data_in:(list_to_carray data_in)
            ~data_out:(list_to_carray data_out))
         () >>= fun () ->

       encode ~kind ~k ~m ~w data parity size
    end
  else
    Lwt.return ()
