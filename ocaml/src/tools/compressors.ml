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
open Slice
open Bytes_descr
open Ctypes
open Foreign

module Snappy = struct

  let int_to_size_t =
    view
      ~read:(fun size -> Unsigned.Size_t.to_int size)
      ~write:(fun i -> Unsigned.Size_t.of_int i)
      size_t

  type status =
    | OK                [@value 0]
    | INVALID_INPUT     [@value 1]
    | BUFFER_TOO_SMALL  [@value 2]
  [@@deriving enum]

  exception Exn of status

  let check_status = function
    | OK -> ()
    | s -> raise (Exn s)

  let status =
    view
      ~read:(fun i -> Option.get_some (status_of_enum i))
      ~write:(fun s -> status_to_enum s)
      int

  let snappy_max_compressed_length =
    foreign
      "snappy_max_compressed_length"
      (int_to_size_t @-> returning int_to_size_t)

  let _snappy_compress_generic : type t1 c1 t2 c2.
    (t1, c1) Bytes_descr.t -> (t2, c2) Bytes_descr.t ->
    release_runtime_lock : bool -> t1 -> t2 =
    fun descr_data descr_res ~release_runtime_lock ->
      let inner =
        foreign
          ~release_runtime_lock
          "snappy_compress"
          (Bytes_descr.get_ctypes descr_data @->
           int_to_size_t @->
           Bytes_descr.get_ctypes descr_res @->
           ptr int_to_size_t @->
           returning status)
      in
      fun data ->
        let len = Bytes_descr.length descr_data data in
        let max_len = snappy_max_compressed_length len in
        let len_res = allocate int_to_size_t max_len in
        let res = Bytes_descr.create descr_res max_len in
        let status =
          inner
            (Bytes_descr.start descr_data data)
            len
            (Bytes_descr.start descr_res res)
            len_res in
        check_status status;
        Bytes_descr.sub descr_res res 0 !@len_res

  let compress_substring s off len =
    let res =
      _snappy_compress_generic
        Bytes_descr.Slice Bytes_descr.Slice
        ~release_runtime_lock:false
        (Slice.make s off len)
    in
    Slice.get_string_unsafe res

  let compress_string s =
    compress_substring s 0 (String.length s)

  let compress_ba_to_string ba =
    let res =
      _snappy_compress_generic
        Bytes_descr.Bigarray Bytes_descr.Slice
        ~release_runtime_lock:false
        ba
    in
    Slice.get_string_unsafe res

  let compress_ba_ba =
    _snappy_compress_generic
      Bytes_descr.Bigarray Bytes_descr.Bigarray

  let _snappy_uncompress_generic : type t1 c1 t2 c2.
    (t1, c1) Bytes_descr.t -> (t2, c2) Bytes_descr.t ->
    release_runtime_lock : bool -> t1 -> t2 =
    fun descr_data descr_res ~release_runtime_lock ->
      let get_uncompressed_length =
        foreign
          "snappy_uncompressed_length"
          (Bytes_descr.get_ctypes descr_data @->
           int_to_size_t @->
           ptr int_to_size_t @->
           returning status)
      in
      let inner =
        foreign
          ~release_runtime_lock
          "snappy_uncompress"
          (Bytes_descr.get_ctypes descr_data @->
           int_to_size_t @->
           Bytes_descr.get_ctypes descr_res @->
           ptr int_to_size_t @->
           returning status)
      in
      fun data ->
        let src = Bytes_descr.start descr_data data in
        let len = Bytes_descr.length descr_data data in
        let res_len = allocate int_to_size_t 0 in
        let status =
          get_uncompressed_length
            src
            len
            res_len in
        check_status status;
        let res = Bytes_descr.create descr_res !@res_len in
        let status =
          inner
            src
            len
            (Bytes_descr.start descr_res res)
            res_len in
        check_status status;
        res

  let uncompress_substring s off len =
    let res =
      _snappy_uncompress_generic
        Bytes_descr.Slice Bytes_descr.Slice
        ~release_runtime_lock:false
        (Slice.make s off len)
    in
    Slice.get_string_unsafe res

  let uncompress_string s =
    uncompress_substring s 0 (String.length s)

  let uncompress_substring_ba s off len =
    _snappy_uncompress_generic
      Bytes_descr.Slice Bytes_descr.Bigarray
      ~release_runtime_lock:false
      (Slice.make s off len)

  let uncompress_string_ba s =
    uncompress_substring_ba s 0 (String.length s)

  let uncompress_ba_ba ~release_runtime_lock =
    _snappy_uncompress_generic
      Bytes_descr.Bigarray Bytes_descr.Bigarray
      ~release_runtime_lock
end

module Bzip2 = struct

  let int_to_unsigned_int =
    view
      ~read:Unsigned.UInt.to_int
      ~write:Unsigned.UInt.of_int
      uint

  type status =
    | OK             [@value 0]
    | RUN_OK         [@value 1]
    | FLUSH_OK       [@value 2]
    | FINISH_OK      [@value 3]
    | STREAM_END     [@value 4]
    | SEQUENCE_ERROR [@value -1]
    | PARAM_ERROR    [@value -2]
    | MEM_ERROR      [@value -3]
    | DATA_ERROR     [@value -4]
    | DATA_ERR_MAGIC [@value -5]
    | IO_ERROR       [@value -6]
    | UNEXPECTED_EOF [@value -7]
    | OUTBUFF_FULL   [@value -8]
    | CONFIG_ERROR   [@value -9]
  [@@deriving enum]

  exception Exn of status

  let check_status = function
    | OK -> ()
    | s -> raise (Exn s)

  let status =
    view
      ~read:(fun i -> Option.get_some (status_of_enum i))
      ~write:(fun s -> status_to_enum s)
      int

  let verbosity = 0
  let workfactor = 0
  let small = 0

  let _compress_generic : type t1 c1 t2 c2.
    (t1, c1) Bytes_descr.t -> (t2, c2) Bytes_descr.t ->
    release_runtime_lock:bool -> ?block:int ->t1 -> t2 =
    fun descr_data descr_res ~release_runtime_lock ->
      let inner =
        (* int BZ2_bzBuffToBuffCompress( char*         dest, *)
        (*                               unsigned int* destLen, *)
        (*                               char*         source, *)
        (*                               unsigned int  sourceLen, *)
        (*                               int           blockSize100k, *)
        (*                               int           verbosity, *)
        (*                               int           workFactor ); *)
        foreign
          "BZ2_bzBuffToBuffCompress"
          ~release_runtime_lock
          (Bytes_descr.get_ctypes descr_res @->
           ptr int_to_unsigned_int @->
           Bytes_descr.get_ctypes descr_data @->
           int_to_unsigned_int @->
           int @-> int @-> int @->
           returning status)
      in
      fun ?(block=9) data ->
        let len = Bytes_descr.length descr_data data in
        let max_len = len + len/100 + 602 in
        let len_res = allocate int_to_unsigned_int max_len in
        (* we prefix the compressed result with its length, hence 4 extra bytes *)
        let dest = Bytes_descr.create descr_res (max_len + 4) in
        let status =
          inner
            (let l = Bytes_descr.sub descr_res dest 4 max_len in
             Bytes_descr.start descr_res l)
            (* (Bytes_descr.start descr_res dest +@ 4) *)
            len_res
            (Bytes_descr.start descr_data data)
            len
            block
            verbosity
            workfactor
        in
        check_status status;
        (* serialize effective length before compressed data *)
        Bytes_descr.set32_prim descr_res dest 0 (Int32.of_int len);
        Bytes_descr.sub descr_res dest 0 (!@len_res + 4)

  let compress_string ?block s =
    let res =
      _compress_generic
        Bytes_descr.Slice Bytes_descr.Slice
        ~release_runtime_lock:false
        (Slice.wrap_string s)
    in
    Slice.get_string_unsafe res

  let compress_ba_to_string ?block ba =
    let res =
      _compress_generic
        Bytes_descr.Bigarray Bytes_descr.Slice
        ~release_runtime_lock:false
        ba
    in
    Slice.get_string_unsafe res

  let compress_string_to_ba ?block s =
    _compress_generic
      Bytes_descr.Slice Bytes_descr.Bigarray
      ~release_runtime_lock:false
      (Slice.wrap_string s)

  let compress_ba_ba =
    _compress_generic
      Bytes_descr.Bigarray Bytes_descr.Bigarray

  let _decompress_generic : type t1 c1 t2 c2.
    (t1, c1) Bytes_descr.t -> (t2, c2) Bytes_descr.t ->
    release_runtime_lock:bool ->t1 -> t2 =
    fun descr_data descr_res ~release_runtime_lock ->
      let inner =
        (* int BZ2_bzBuffToBuffDecompress( char*         dest, *)
        (*                                 unsigned int* destLen, *)
        (*                                 char*         source, *)
        (*                                 unsigned int  sourceLen, *)
        (*                                 int           small, *)
        (*                                 int           verbosity ); *)
        foreign
          "BZ2_bzBuffToBuffDecompress"
          ~release_runtime_lock
          (Bytes_descr.get_ctypes descr_res @->
           ptr int_to_unsigned_int @->
           Bytes_descr.get_ctypes descr_data @->
           int_to_unsigned_int @->
           int @->
           int @->
           returning status)
      in
      fun data ->
        let len = Bytes_descr.length descr_data data - 4 in
        let res_len =
          Int32.to_int
            (Bytes_descr.get32_prim descr_data data 0) in
        let res = Bytes_descr.create descr_res res_len in
        let status =
          inner
            (Bytes_descr.start descr_res res)
            (allocate int_to_unsigned_int res_len)
            (let l = Bytes_descr.sub descr_data data 4 len in
             Bytes_descr.start descr_data l)
            len
            small
            verbosity in
        check_status status;
        res

  let decompress_substring s off len =
    let res =
      _decompress_generic
        Bytes_descr.Slice Bytes_descr.Slice
        ~release_runtime_lock:false
        (Slice.make s off len)
    in
    Slice.get_string_unsafe res

  let decompress_string s =
    decompress_substring s 0 (String.length s)

  let decompress_substring_ba s off len =
    _decompress_generic
      Bytes_descr.Slice Bytes_descr.Bigarray
      ~release_runtime_lock:false
      (Slice.make s off len)

  let decompress_string_ba s = decompress_substring_ba s 0 (String.length s)

  let decompress_ba_string ba =
    let r =
      _decompress_generic
        Bytes_descr.Bigarray Bytes_descr.Slice
        ~release_runtime_lock:false
        ba
    in
    Slice.get_string_unsafe r

  let decompress_ba_ba =
    _decompress_generic
      Bytes_descr.Bigarray Bytes_descr.Bigarray
end

open Alba_compression.Compression

let decompress =
  (* big array to big array, detached *)
  function
  | NoCompression -> Lwt.return
  | Snappy ->
    Lwt_preemptive.detach
      (Snappy.uncompress_ba_ba ~release_runtime_lock:true)
  | Bzip2 ->
    Lwt_preemptive.detach
      (Bzip2.decompress_ba_ba ~release_runtime_lock:true)

let compress = function
  | NoCompression -> Lwt.return
  | Snappy ->
    Lwt_preemptive.detach
      (Snappy.compress_ba_ba ~release_runtime_lock:true)
  | Bzip2 ->
    Lwt_preemptive.detach
      (Bzip2.compress_ba_ba ~release_runtime_lock:true)

