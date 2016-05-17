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

open Ctypes
open Foreign
open Ctypes_helper

let gen_rs_matrix =
  let inner =
    (* @brief Generate a matrix of coefficients to be used for encoding. *)

    (* Vandermonde matrix example of encoding coefficients where high portion of *)
    (* matrix is identity matrix I and lower portion is constructed as 2^{i*(j-k+1)} *)
    (* i:{0,k-1} j:{k,m-1}. Commonly used method for choosing coefficients in *)
    (* erasure encoding but does not guarantee invertable for every sub matrix.  For *)
    (* large k it is possible to find cases where the decode matrix chosen from *)
    (* sources and parity not in erasure are not invertable. Users may want to *)
    (* adjust for k > 5. *)

    (* @param a  [mxk] array to hold coefficients *)
    (* @param m  number of rows in matrix corresponding to srcs + parity. *)
    (* @param k  number of columns in matrix corresponding to srcs. *)
    (* @returns  none *)

    (* void gf_gen_rs_matrix(unsigned char *a, int m, int k); *)
    foreign
      "gf_gen_rs_matrix"
      (ptr uchar @-> int @-> int @-> returning void)
  in
  fun ~k ~m ->
  let a = allocate_n uchar ~count:(k*m) in
  let () = inner a m k in
  a

let init_tables =
  let inner =
    (* @brief Initialize tables for fast Erasure Code encode and decode. *)

    (* Generates the expanded tables needed for fast encode or decode for erasure *)
    (* codes on blocks of data.  32bytes is generated for each input coefficient. *)

    (* @param k      The number of vector sources or rows in the generator matrix *)
    (*               for coding. *)
    (* @param rows   The number of output vectors to concurrently encode/decode. *)
    (* @param a      Pointer to sets of arrays of input coefficients used to encode *)
    (*               or decode data. *)
    (* @param gftbls Pointer to start of space for concatenated output tables *)
    (*               generated from input coefficients.  Must be of size 32*k*rows. *)
    (* @returns none *)

    (* void ec_init_tables(int k, int rows, unsigned char* a, unsigned char* gftbls); *)
    foreign
      "ec_init_tables"
      (int @-> int @-> ptr uchar @-> ptr uchar @-> returning void)
  in
  fun ~k ~rows a ->
  let gftbls = allocate_n uchar ~count:(32*k*rows) in
  let () =
    inner
      k rows
      a gftbls
  in
  gftbls

let rs_vandermonde_matrix_from_jerasure ~k ~rows cm =
  Bytes.init
    (rows*k)
    (fun i -> Char.chr !@(cm +@ i))
  |> Lwt_bytes.of_string


let gen_rs_vandermonde_matrix_jerasure ~k ~m =
  let rows = k + m in
  with_free
    (fun () ->
     Jerasure.reed_sol_big_vandermonde_distribution_matrix
       ~rows ~cols:k ~w:8)
    (fun inner ->
     rs_vandermonde_matrix_from_jerasure ~k ~rows inner)

let encode_data ~release_runtime_lock =
  let inner =
    (*rief Generate or decode erasure codes on blocks of data, runs appropriate version. *)

    (* Given a list of source data blocks, generate one or multiple blocks of *)
    (* encoded data as specified by a matrix of GF(2^8) coefficients. When given a *)
    (* suitable set of coefficients, this function will perform the fast generation *)
    (* or decoding of Reed-Solomon type erasure codes. *)

    (* This function determines what instruction sets are enabled and *)
    (* selects the appropriate version at runtime. *)

    (* @param len    Length of each block of data (vector) of source or dest data. *)
    (* @param k      The number of vector sources or rows in the generator matrix *)
    (* 		 for coding. *)
    (* @param rows   The number of output vectors to concurrently encode/decode. *)
    (* @param gftbls Pointer to array of input tables generated from coding *)
    (* 		 coefficients in ec_init_tables(). Must be of size 32*k*rows *)
    (* @param data   Array of pointers to source input buffers. *)
    (* @param coding Array of pointers to coded output buffers. *)
    (* @returns none *)

    (* void ec_encode_data(int len, int k, int rows, unsigned char *gftbls, unsigned char **data, *)
    (* 		    unsigned char **coding); *)
    foreign
      ~release_runtime_lock
      "ec_encode_data"
      (int @-> int @-> int
       @-> ptr uchar @-> ptr (ptr char) @-> ptr (ptr char)
       @-> returning void)
  in
  fun ~len ~k ~rows ~gftbls ~data_in ~data_out ->
  inner
    len k rows
    gftbls
    data_in
    data_out
