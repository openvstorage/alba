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
open Slice
open Lwt_bytes2
open Ctypes
open Foreign
open Bytes_descr

(*
  Have look at https://www.gnupg.org/documentation/manuals/gcrypt/Working-with-cipher-handles.html
  and /usr/include/gcrypt.h for the missing details.
*)

let int_to_uint =
  view
    ~read:Unsigned.UInt.to_int
    ~write:Unsigned.UInt.of_int
    uint

let int_to_size_t =
  view
    ~read:Unsigned.Size_t.to_int
    ~write:Unsigned.Size_t.of_int
    size_t

exception OperationOnInvalidObject

module Error = struct
  type t = int
  let t = int_to_uint

  exception Error of t * string * string * string

  let get_string =
    let inner =
      (* Function: const char * gcry_strerror (gcry_error_t err)
       *
       * The function gcry_strerror returns a pointer to a statically allocated string
       * containing a description of the error code contained in the error value err.
       * This string can be used to output a diagnostic message to the user.
       *)
      foreign
        "gcry_strerror"
        (int @-> returning string)
    in
    inner

  let get_source =
    let inner =
      (* Function: const char * gcry_strsource (gcry_error_t err)
       *
       * The function gcry_strsource returns a pointer to a statically allocated string
       * containing a description of the error source contained in the error value err.
       * This string can be used to output a diagnostic message to the user. 
       *)
      foreign
        "gcry_strsource"
        (int @-> returning string)
    in
    inner

  let check ~location t =
    if t <> 0
    then raise (Error (t, location, get_string t, get_source t))

  let check_lwt ~location t =
    if t <> 0
    then Lwt.fail (Error (t, location, get_string t, get_source t))
    else Lwt.return ()
end

let check_version version =
  let inner =
    (* const char * gcry_check_version (const char *req_version) *)
    foreign
      "gcry_check_version"
      (ocaml_string @-> returning string_opt)
  in
  inner (ocaml_string_start version)

type cmd =
  | DISABLE_SECMEM           [@value 37]
  | INITIALIZATION_FINISHED  [@value 38]
[@@deriving enum]

let cmd =
  view
    ~read:(fun _ -> failwith "Can't read commands")
    ~write:cmd_to_enum
    int

let control =
  (* ctypes doesn't support variadic functions (for now), see
     https://github.com/ocamllabs/ocaml-ctypes/issues/103
     for more info *)
  let inner =
    (* gcry_error_t gcry_control (enum gcry_ctl_cmds CMD, ...); *)
    foreign
      "gcry_control"
      (cmd @-> int @-> returning Error.t)
  in
  fun cmd1 cmd2 ->
    let err = inner cmd1 cmd2 in
    Error.check ~location:"gcrypt.control" err


let () =
  (* https://www.gnupg.org/documentation/manuals/gcrypt/Initializing-the-library.html *)

  (* /* Version check should be the very first call because it *)
  (*    makes sure that important subsystems are intialized. */ *)
  let version_o : string option = check_version "1.6.0" in
  assert (version_o <> None);

  (* /* Disable secure memory.  */ *)
  (* gcry_control (GCRYCTL_DISABLE_SECMEM, 0); *)
  control DISABLE_SECMEM 0;

  (* /* ... If required, other initialization goes here.  */ *)

  (* /* Tell Libgcrypt that initialization has completed. */ *)
  (* gcry_control (GCRYCTL_INITIALIZATION_FINISHED, 0); *)
  control INITIALIZATION_FINISHED 0


module Padding = struct

  (* returns a new big_array *)
  let pad data block_len =
    (* pad from Bigstring_slice.t to bigarray *)
    let used_len = Bigstring_slice.length data in
    let pad_len = (used_len/block_len + 1) * block_len in
    let res = Lwt_bytes.create pad_len in
    (let open Bigstring_slice in
     Lwt_bytes.blit data.bs data.offset res 0 used_len);
    let n = pad_len - used_len in
    assert (n > 0 && n <= block_len);
    Lwt_bytes.fill res used_len n (Char.chr n);
    res

  let unpad data =
    (* unpad from Lwt_bytes to Bigstring_slice (in place) *)
    let len = Lwt_bytes.length data in
    let cnt_char = Lwt_bytes.get data (len - 1) in
    let cnt = Char.code cnt_char in
    if cnt = 0 || cnt > len then failwith "bad padding";

    for i = len - cnt to len - 1 do
      if cnt_char <> Lwt_bytes.get data i
      then failwith "bad padding'"
    done;

    Bigstring_slice.from_bigstring
      data
      0
      (len - cnt)

end

module Cipher = struct

  type hd_t = {
    ptr : unit ptr;
    mutable valid : bool;
  }
  let hd_t =
    view
      ~read:(fun ptr -> { ptr; valid = true; })
      ~write:(
        fun { ptr; valid; } ->
          if valid
          then ptr
          else raise OperationOnInvalidObject)
      (ptr void)

  type algo =
    | AES256       [@value 9]
  [@@deriving enum, show]

  let algo =
    view
      ~read:(fun i -> Option.get_some (algo_of_enum i))
      ~write:algo_to_enum
      int

  type mode =
    | CBC          [@value 3]
    | CTR          [@value 6]
  [@@deriving enum, show]

  let mode =
    view
      ~read:(fun i -> Option.get_some (mode_of_enum i))
      ~write:mode_to_enum
      int

  type flag =
    | SECURE       [@value 1]
    | ENABLE_SYNC  [@value 2]
    | CBC_CTS      [@value 4]
    | CBC_MAC      [@value 8]

  type flags = flag list
  let flags =
    view
      ~read:(fun _ -> failwith "TODO can't read flags, only write")
      ~write:
        (fun flags ->
           let i =
             List.fold_left
               (fun acc flag -> acc lor flag)
               0
               flags
           in
           Unsigned.UInt.of_int i)
      uint

  let open_ =
    (* gcry_error_t
         gcry_cipher_open (gcry_cipher_hd_t *hd,
                           int algo,
                           int mode,
                           unsigned int flags) *)
    let inner =
      foreign
        "gcry_cipher_open"
        (ptr hd_t @-> algo @-> mode @-> flags @-> returning Error.t)
    in
    fun algo mode flags ->
      let res = allocate hd_t { ptr = null;
                                valid = true; } in
      let err = inner res algo mode flags in
      Error.check ~location:"Cipher.open" err;
      !@res

  let close =
    (* void gcry_cipher_close (gcry_cipher_hd_t h) *)
    let f =
      foreign
        "gcry_cipher_close"
        (hd_t @-> returning void)
    in
    fun t ->
      f t;
      t.valid <- false

  let set_key =
    (* gcry_error_t gcry_cipher_setkey (gcry_cipher_hd_t h, const void *k, size_t l) *)
    let inner =
      foreign
        "gcry_cipher_setkey"
        (hd_t @-> ocaml_string @-> int_to_size_t @-> returning Error.t)
    in
    fun t key ->
      let err = inner t (ocaml_string_start key) (String.length key) in
      Error.check ~location:"Cipher.set_key" err

  let create key algo mode flags =
    let t = open_ algo mode flags in
    set_key t key;
    t

  let with_t key algo mode flags f =
    let t = create key algo mode flags in
    finalize
      (fun () -> f t)
      (fun () -> close t)

  let with_t_lwt key algo mode flags f =
    let t = create key algo mode flags in
    Lwt.finalize
      (fun () -> f t)
      (fun () ->
         close t;
         Lwt.return ())

  let set_iv =
    (* gcry_error_t gcry_cipher_setiv (gcry_cipher_hd_t h, const void *k, size_t l) *)
    let inner =
      foreign
        "gcry_cipher_setiv"
        (hd_t @-> ocaml_string @-> int_to_size_t @-> returning Error.t)
    in
    fun t iv ->
      let err = inner t (ocaml_string_start iv) (String.length iv) in
      Error.check ~location:"Cipher.set_iv" err

  let set_ctr =
    (* gcry_error_t gcry_cipher_setctr (gcry_cipher_hd_t h, const void *c, size_t l)
     *
     * Set the counter vector used for encryption or decryption.
     * The counter is passed as the buffer c of length l bytes and copied to internal data structures.
     * The function checks that the counter matches the requirement of the selected algorithm
     * (i.e., it must be the same size as the block size).
     *)
    let inner =
      foreign
        "gcry_cipher_setctr"
        (hd_t @-> ocaml_string @-> int_to_size_t @-> returning Error.t)
    in
    fun t ctr ->
    let err = inner t (ocaml_string_start ctr) (String.length ctr) in
    Error.check ~location:"Cipher.set_ctr" err

  let encrypt =
    (* gcry_error_t
         gcry_cipher_encrypt (
           gcry_cipher_hd_t h,
           unsigned char *out,
           size_t outsize,
           const unsigned char *in,
           size_t inlen)

       gcry_cipher_encrypt is used to encrypt the data. This function can either
       work in place or with two buffers. It uses the cipher context already setup
       and described by the handle h. There are 2 ways to use the function: If in
       is passed as NULL and inlen is 0, in-place encryption of the data in out or
       length outsize takes place. With in being not NULL, inlen bytes are encrypted
       to the buffer out which must have at least a size of inlen. outsize must be
       set to the allocated size of out, so that the function can check that there is
       sufficient space. Note that overlapping buffers are not allowed.

       Depending on the selected algorithms and encryption mode, the length of the
       buffers must be a multiple of the block size.

       The function returns 0 on success or an error code.
    *)
    let inner =
      foreign
        "gcry_cipher_encrypt"
        ~release_runtime_lock:true
        (hd_t @->
         ptr char @-> int_to_size_t @->
         ptr char @-> int_to_size_t @->
         returning Error.t)
    in
    fun t data ->
      let len = Lwt_bytes.length data in
      let open Lwt.Infix in
      Lwt_preemptive.detach
        (fun () ->
           inner
             t
             (bigarray_start array1 data) len
             (from_voidp char null) 0)
        ()
      >>= Error.check_lwt ~location:"Cipher.encrypt"

  let decrypt ~release_runtime_lock =
    (* gcry_error_t
         gcry_cipher_decrypt (
           gcry_cipher_hd_t h,
           unsigned char *out,
           size_t outsize,
           const unsigned char *in,
           size_t inlen)

       gcry_cipher_decrypt is used to decrypt the data. This function can either
       work in place or with two buffers. It uses the cipher context already setup
       and described by the handle h. There are 2 ways to use the function: If in
       is passed as NULL and inlen is 0, in-place decryption of the data in out or
       length outsize takes place. With in being not NULL, inlen bytes are decrypted
       to the buffer out which must have at least a size of inlen. outsize must be
       set to the allocated size of out, so that the function can check that there is
       sufficient space. Note that overlapping buffers are not allowed.

       Depending on the selected algorithms and encryption mode, the length of the
       buffers must be a multiple of the block size.

       The function returns 0 on success or an error code.
    *)
    let inner =
      foreign
        "gcry_cipher_decrypt"
        ~release_runtime_lock
        (hd_t @->
         ptr char @-> int_to_size_t @->
         ptr char @-> int_to_size_t @->
         returning Error.t)
    in
    fun t (data : Lwt_bytes.t) offset length ->
    inner
      t
      (bigarray_start array1 data +@ offset) length
      (from_voidp char null) 0

  let decrypt_detached t data offset length =
    let open Lwt.Infix in
    Lwt_preemptive.detach
      (fun () -> decrypt ~release_runtime_lock:true
                         t data offset length)
      ()
    >>= Error.check_lwt ~location:"Cipher.decrypt_detached"

  let _burn_buf = Lwt_bytes.create 16

  let set_ctr_with_offset t ctr offset =
    let block_len = 16 in
    assert (String.length ctr = block_len);
    let ctr = Bytes.copy ctr in

    let skip_blocks = offset / block_len in
    let skip_blocks64 = Int64.of_int skip_blocks in

    (* bump cntr_low *)
    let cntr_low = EndianBytes.BigEndian.get_int64 ctr 8 in
    let cntr_low' = Int64.add cntr_low skip_blocks64 in
    let () = EndianBytes.BigEndian.set_int64 ctr 8 cntr_low' in

    (* update cntr_high if we have overflow *)
    if cntr_low' >= 0L && cntr_low' < skip_blocks64
    then
      begin
        let cntr_high = EndianBytes.BigEndian.get_int64 ctr 0 in
        EndianBytes.BigEndian.set_int64 ctr 0 (Int64.succ cntr_high)
      end;

    set_ctr t ctr;

    let to_burn = offset - (skip_blocks * block_len) in
    if to_burn > 0
    then
      decrypt
        ~release_runtime_lock:false
        t
        _burn_buf
        0 to_burn
      |> Error.check ~location:"Cipher.set_ctr_with_offset decrypt"

end

module Digest = struct

  type hd_t = {
    ptr : unit ptr;
    mutable valid : bool;
  }
  let hd_t =
    view
      ~read:(fun ptr -> { ptr; valid = true; })
      ~write:(
        fun { ptr; valid; } ->
          if valid
          then ptr
          else raise OperationOnInvalidObject)
      (ptr void)

  type algo =
    | SHA1     [@value 2]
    | SHA256   [@value 8]
  [@@deriving enum]
  let algo =
    view
      ~read:(fun i -> Option.get_some (algo_of_enum i))
      ~write:algo_to_enum
      int

  let open_ =
    (* gcry_error_t
         gcry_md_open
           (gcry_md_hd_t *hd,
            int algo,
            unsigned int flags) *)
    let inner =
      foreign
        "gcry_md_open"
        (ptr hd_t @-> algo @-> int @-> returning Error.t)
    in
    fun algo ->
      let res = allocate hd_t { ptr = null;
                                valid = true; } in
      let err = inner res algo 0 in
      Error.check ~location:"Digest.open" err;
      !@res

  let close =
    (* void gcry_md_close (gcry_md_hd_t h) *)
    let inner =
      foreign
        "gcry_md_close"
        (hd_t @-> returning void)
    in
    fun t ->
      inner t;
      t.valid <- false

  let enable =
    (* gcry_error_t gcry_md_enable (gcry_md_hd_t h, int algo) *)
    foreign
      "gcry_md_enable"
      (hd_t @-> algo @-> returning Error.t)

  let set_key_ : type t1 c1.
    (t1, c1) Bytes_descr.t -> hd_t -> t1 -> unit =
    fun descr_key ->
      let inner =
        (* gcry_error_t gcry_md_setkey (gcry_md_hd_t h, const void *key, size_t keylen) *)
        foreign
          "gcry_md_setkey"
          (hd_t @-> Bytes_descr.get_ctypes descr_key @-> int_to_size_t @-> returning Error.t)
      in
      fun t key ->
        let err =
          inner
            t
            (Bytes_descr.start descr_key key)
            (Bytes_descr.length descr_key key)
        in
        Error.check ~location:"Digest.set_key" err

  let set_key t key = set_key_ Bytes_descr.Slice t (Slice.wrap_string key)

  let write_ : type t1 c1. (t1, c1) Bytes_descr.t ->
    release_runtime_lock:bool -> hd_t ->
    t1 -> unit =
    fun descr ~release_runtime_lock ->
      let inner =
        (* void gcry_md_write (gcry_md_hd_t h, const void *buffer, size_t length) *)
        foreign
          "gcry_md_write"
          ~release_runtime_lock
          (hd_t @-> Bytes_descr.get_ctypes descr @-> int_to_size_t @-> returning void)
      in
      fun t data ->
        inner
          t
          (Bytes_descr.start descr data)
          (Bytes_descr.length descr data)

  let write_slice t = write_ Bytes_descr.Slice t ~release_runtime_lock:false

  let write t data =
    write_slice
      t
      (Slice.wrap_string data)

  let write_bs t ~release_runtime_lock data =
    write_
      Bytes_descr.Bigstring_slice
      t
      ~release_runtime_lock
      data

  let final t = ()
  (* apparently this is a macro, so the following implementation didn't work *)
  (* in the header I found this:
     /* Finalize the digest calculation.  This is not really needed because
        gcry_md_read() does this implicitly. */
        #define gcry_md_final(a) \
                gcry_md_ctl ((a), GCRYCTL_FINALIZE, NULL, 0)
  *)
  (* let final = *)
  (*   (\* void gcry_md_final (gcry_md_hd_t h) *\) *)
  (*   foreign *)
  (*     "gcry_md_final" *)
  (*     (hd_t @-> returning void) *)

  let algo_len =
    (* unsigned int gcry_md_get_algo_dlen (int algo) *)
    foreign
      "gcry_md_get_algo_dlen"
      (algo @-> returning int_to_uint)

  let read =
    let inner =
      (* unsigned char * gcry_md_read (gcry_md_hd_t h, int algo) *)
      foreign
        "gcry_md_read"
        (hd_t @-> algo @-> returning (ptr char))
    in
    fun t algo ->
      let res = inner t algo in
      let length = algo_len algo in
      if (to_voidp res) = null
      then None
      else Some (string_from_ptr res ~length)
end
