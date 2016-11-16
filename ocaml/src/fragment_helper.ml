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

open Prelude
open Slice
open Lwt_bytes2
open Nsm_model
open Encryption
open Gcrypt
open Lwt.Infix

let get_iv key ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id =
  (*
     deterministic iv for fragment encryption
     this way we get security without needing to store the iv in the manifest / recovery info

     concatenate object_id chunk_id fragment_id |>
     encrypt AES key CBC |>
     take last block
 *)
  let fragment_id =
    if ignore_fragment_id
    then 0
    else fragment_id
  in
  let s =
    serialize
      (Llio.tuple3_to
         Llio.string_to
         Llio.int_to
         Llio.int_to)
      (object_id, chunk_id, fragment_id)
  in

  let block_len = 16 in
  let bs =
    let x = Lwt_bytes.of_string s in
    finalize
      (fun () -> Padding.pad (Bigstring_slice.wrap_bigstring x) block_len)
      (fun () -> Lwt_bytes.unsafe_destroy x)
  in

  Cipher.with_t_lwt key Cipher.AES256 Cipher.CBC []
    (fun cipher -> Cipher.encrypt cipher bs) >>= fun () ->

  let res = Str.last_chars (Lwt_bytes.to_string bs) block_len in
  Lwt.return res

(* consumes the input and returns a big_array *)
let maybe_encrypt
    encryption
    ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id
    plain =
  let open Encryption in
  match encryption with
  | NoEncryption ->
    Lwt.return (plain, None)
  | AlgoWithKey (AES (CBC, L256) as algo, key) ->
    verify_key_length algo key;
    let block_len = block_length algo in
    let bs =
      finalize
        (fun () -> Padding.pad (Bigstring_slice.wrap_bigstring plain) block_len)
        (fun () -> Lwt_bytes.unsafe_destroy plain)
    in
    get_iv key ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id >>= fun iv ->
    Cipher.with_t_lwt
      key Cipher.AES256 Cipher.CBC []
      (fun cipher ->
        Cipher.set_iv cipher iv;
        Cipher.encrypt cipher bs) >>= fun () ->
    Lwt.return (bs, None)
  | AlgoWithKey (AES (CTR, L256) as algo, key) ->
    verify_key_length algo key;
    let iv = get_random_string 16 in
    Cipher.with_t_lwt
      key Cipher.AES256 Cipher.CTR []
      (fun cipher ->
        Cipher.set_ctr cipher iv;
        Cipher.encrypt cipher plain) >>= fun () ->
    Lwt.return (plain, Some iv)

(* consumes the input and returns a bigstring_slice *)
let maybe_decrypt
    encryption
    ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id
    ~fragment_ctr
    data =
  let open Encryption in
  match encryption with
  | NoEncryption ->
    Lwt.return (Bigstring_slice.wrap_bigstring data)
  | AlgoWithKey (algo, key) ->
     let encrypt mode set_iv_ctr =
       Encryption.verify_key_length algo key;
       Cipher.with_t_lwt
         key Cipher.AES256 Cipher.CBC []
          (fun cipher ->
            set_iv_ctr cipher;
            Cipher.decrypt_detached
                  cipher
                  data 0 (Lwt_bytes.length data)
          )
     in
     begin match algo with
     | AES (CBC, L256) ->
        get_iv key ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id >>= fun iv ->
        encrypt Cipher.CBC (fun h -> Cipher.set_iv h iv) >>= fun () ->
        Lwt.return (Padding.unpad data)
     | AES (CTR, L256) ->
        encrypt Cipher.CTR (fun h -> Cipher.set_ctr h (Option.get_some fragment_ctr)) >>= fun () ->
        Lwt.return (Bigstring_slice.wrap_bigstring data)
    end

let maybe_partial_decrypt
      encryption
      ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id
      ~fragment_ctr
      (data, offset, length) ~fragment_offset =
  let open Encryption in
  match encryption with
  | NoEncryption -> Lwt.return ()
  | AlgoWithKey (algo, key) ->
     begin
       match algo with
       | AES (CBC, _) -> Lwt.fail_with "can't do partial decrypt for AES CBC"
       | AES (CTR, L256) ->
          Cipher.(with_t_lwt
                    key AES256 CTR []
                    (fun handle ->
                      set_ctr_with_offset handle (Option.get_some fragment_ctr) fragment_offset;
                      decrypt_detached handle data offset length))
     end

(* returns a new bigarray *)
let maybe_compress compression fragment_data =
  let open Lwt.Infix in
  Compressors.compress compression fragment_data >>= fun r ->
  Lwt_log.debug_f
    "compression: %s (%i => %i)"
    ([%show: Compression.t] compression)
    (Bigstring_slice.length fragment_data)
    (Lwt_bytes.length r) >>= fun () ->
  Lwt.return r

let maybe_decompress compression compressed =
  let open Lwt.Infix in
  let compressed_length = Bigstring_slice.length compressed in
  Compressors.decompress compression compressed >>= fun r ->
  Lwt_log.debug_f
     "decompression: %s (%i => %i)"
     ([%show: Compression.t] compression)
     compressed_length
     (Lwt_bytes.length r) >>= fun () ->
  Lwt.return r


let verify fragment_data checksum =
  let algo = Checksum.algo_of checksum in
  let hash = Hashes.make_hash algo in
  hash # update_lwt_bytes_detached
       fragment_data
       0 (Lwt_bytes.length fragment_data) >>= fun () ->
  let checksum2 = hash # final () in
  Lwt.return (checksum2 = checksum)

let verify' fragment_data checksum =
  let algo = Checksum.algo_of checksum in
  let hash = Hashes.make_hash algo in
  let open Slice in
  hash # update_substring
    fragment_data.buf
    fragment_data.offset
    fragment_data.length;
  let checksum2 = hash # final () in
  Lwt.return (checksum2 = checksum)

(* returns a new big_array *)
let pack_fragment
    (fragment : Bigstring_slice.t)
    ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id
    compression
    encryption
    checksum_algo
  =
  with_timing_lwt
    (fun () ->
       maybe_compress compression fragment
       >>= fun compressed ->
       maybe_encrypt
         ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id
         encryption
         compressed)
  >>= fun (t_compress_encrypt, (final_data, fragment_ctr)) ->

  with_timing_lwt
    (fun () ->
     let hash = Hashes.make_hash checksum_algo in
     hash # update_lwt_bytes_detached
          final_data
          0
          (Lwt_bytes.length final_data) >>= fun () ->
       Lwt.return (hash # final ()))
  >>= fun (t_hash, checksum) ->

  Lwt.return (final_data, t_compress_encrypt, t_hash, checksum, fragment_ctr)

let chunk_to_data_fragments ~chunk ~chunk_size ~k =
  let fragment_size = chunk_size / k in
  let data_fragments =
    let rec inner acc = function
      | 0 ->
        acc
      | n ->
        let fragment =
          let pos = (n-1) * fragment_size in
          Bigstring_slice.from_bigstring chunk pos fragment_size
        in
        inner (fragment :: acc) (n - 1) in
    inner [] k in
  data_fragments

(* The lifetime of the returned data fragments is
   determined by the lifetime of the passed in chunk.
   The returned coding fragments are freshly created
   and should thus be freed by the consumer of this function.
 *)
let chunk_to_fragments_ec
    ~chunk ~chunk_size
    ~k ~m ~w' =

  let fragment_size = chunk_size / k in

  Lwt_log.debug_f
    "chunk_to_fragments: chunk_size = %i ; fragment_size=%i"
    chunk_size fragment_size >>= fun () ->

  assert (chunk_size mod (Fragment_size_helper.fragment_multiple * k) = 0);

  let data_fragments =
    chunk_to_data_fragments
      ~chunk
      ~chunk_size
      ~k
  in

  let coding_fragments =
    let rec inner acc = function
      | 0 -> acc
      | n ->
        let fragment = Bigstring_slice.create fragment_size in
        inner (fragment :: acc) (n - 1) in
    inner [] m
  in

  Erasure.encode
    ~k ~m ~w:w'
    data_fragments
    coding_fragments
    fragment_size >>= fun () ->

  Lwt.return (data_fragments, coding_fragments)

(* returns new bigarrays
 * replication (k=1) is a bit special though
 *)
let chunk_to_packed_fragments
    ~object_id ~chunk_id
    ~chunk ~chunk_size
    ~k ~m ~w'
    ~compression ~encryption ~fragment_checksum_algo
  =
  if k = 1
  then
    begin
      let fragment = Bigstring_slice.wrap_bigstring chunk in
      pack_fragment
        fragment
        ~object_id ~chunk_id ~fragment_id:0 ~ignore_fragment_id:true
        compression encryption fragment_checksum_algo
      >>= fun (packed, f1, f2, cs, ctr) ->

      let rec build_result acc = function
        | 0 -> acc
        | n ->
          let fragment_id = n - 1 in
          let acc' = (fragment_id, fragment, (packed, f1, f2, cs, ctr)) :: acc in
          build_result
            acc'
            (n-1)
      in
      Lwt.return ([ fragment; ], build_result [] (k+m))
    end
  else
    begin
      chunk_to_fragments_ec
        ~chunk ~chunk_size
        ~k ~m ~w' >>= fun (data_fragments, coding_fragments) ->
      Lwt.finalize
        (fun () ->
         let all_fragments = List.append data_fragments coding_fragments in
         Lwt_list.mapi_p
           (fun fragment_id fragment ->
            pack_fragment
              fragment
              ~object_id ~chunk_id ~fragment_id ~ignore_fragment_id:false
              compression encryption fragment_checksum_algo
            >>= fun (packed, f1, f2, cs, ctr) ->
            Lwt.return (fragment_id, fragment, (packed, f1, f2, cs, ctr)))
           all_fragments >>= fun packed_fragments ->
         Lwt.return (data_fragments, packed_fragments))
        (fun () ->
         List.iter
           (fun bss -> Lwt_bytes.unsafe_destroy bss.Bigstring_slice.bs)
           coding_fragments;
         Lwt.return ())
    end
