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
open Lwt_bytes2

module ReadBuffer = struct
    type t = { buf : Lwt_bytes.t;
               mutable pos : int; }

    type 'a deserializer = t -> 'a

    let make_buffer buf pos = { buf; pos; }

    let advance_pos buf delta = buf.pos <- buf.pos + delta

    let buffer_done buf = buf.pos = Lwt_bytes.length buf.buf

    let deserialize ?(pos = 0) deserializer buf =
      deserializer { buf; pos; }

    let deserialize' ?(pos = 0) deserializer (buf : Lwt_bytes.t) =
      deserializer { buf = buf;
                     pos = pos; }

    let unit_from buf = ()

    let maybe_from_buffer a_from default buf =
      if buffer_done buf
      then default
      else
        let res = a_from buf in
        assert (buffer_done buf);
        res

    let pair_from a_from b_from buf =
      let a = a_from buf in
      let b = b_from buf in
      (a, b)
    let tuple3_from a_from b_from c_from buf =
      let a = a_from buf in
      let b = b_from buf in
      let c = c_from buf in
      (a, b, c)
    let tuple4_from a_from b_from c_from d_from buf =
      let a = a_from buf in
      let b = b_from buf in
      let c = c_from buf in
      let d = d_from buf in
      (a, b, c, d)
    let tuple5_from a_from b_from c_from d_from e_from buf =
      let a = a_from buf in
      let b = b_from buf in
      let c = c_from buf in
      let d = d_from buf in
      let e = e_from buf in
      (a, b, c, d, e)

    let int32_from buf =
      let r = get32_prim' buf.buf buf.pos in
      advance_pos buf 4;
      r

    let int64_from buf =
      let r = get64_prim' buf.buf buf.pos in
      advance_pos buf 8;
      r

    let float_from buf =
      int64_from buf |> Int64.float_of_bits

    let int_from buf = int32_from buf |> Int32.to_int

    let char_from buf =
      let r = Lwt_bytes.get buf.buf buf.pos in
      advance_pos buf 1;
      r

    let int8_from buf =
      Char.code (char_from buf)

    let bool_from buf =
      match char_from buf with
      | '\x00' -> false
      | '\x01' -> true
      | k -> raise_bad_tag "bool" (Char.code k)

    let option_from a_from buf =
      match bool_from buf with
      | false -> None
      | true -> Some (a_from buf)

    let string_from buf =
      let len = int_from buf in
      let bs = Bytes.create len in
      Lwt_bytes.blit_to_bytes buf.buf buf.pos bs 0 len;
      advance_pos buf len;
      bs

    let bigstring_slice_from : Bigstring_slice.t deserializer =
      fun buf ->
      let len = int_from buf in
      let r = Bigstring_slice.from_bigstring buf.buf buf.pos len in
      advance_pos buf len;
      r

    let counted_list_from e_from buf =
      let size = int_from buf in
      let rec loop acc = function
        | 0 -> acc
        | i -> let e = e_from buf in
               loop (e::acc) (i-1)
      in
      size, (loop [] size)

    let counted_list_more_from e_from =
      pair_from
        (counted_list_from e_from)
        bool_from

    let list_from e_from buf = counted_list_from e_from buf |> snd

    let hashtbl_from kv_from buf =
      let len = int_from buf in
      let r = Hashtbl.create len in
      let rec loop = function
        | 0 -> r
        | i -> let (k,v) = kv_from buf in
               let () = Hashtbl.add r k v in
               loop (i-1)
      in
      loop len
  end

module WriteBuffer = struct
    type t = { mutable buf : Lwt_bytes.t;
               mutable pos : int; }

    let make ~length =
      { buf = Lwt_bytes.create length;
        pos = 0; }

    type 'a serializer = t -> 'a -> unit

    let ensure_space buf delta =
      let pos = buf.pos in
      let len = Lwt_bytes.length buf.buf in
      if pos + delta > len
      then
        begin
          let newbuf = Lwt_bytes.create (max (len + delta) (2*len)) in
          let oldbuf = buf.buf in
          Lwt_bytes.blit oldbuf 0 newbuf 0 pos;
          buf.buf <- newbuf;
          Lwt_bytes.unsafe_destroy oldbuf
        end

    let advance_pos buf delta =
      buf.pos <- buf.pos + delta

    let with_ buf delta f =
      ensure_space buf delta;
      f ();
      advance_pos buf delta

    let unit_to buf () = ()

    let pair_to a_to b_to buf (a, b) =
      a_to buf a;
      b_to buf b
    let tuple3_to a_to b_to c_to buf (a, b, c) =
      a_to buf a;
      b_to buf b;
      c_to buf c
    let tuple4_to a_to b_to c_to d_to buf (a, b, c, d) =
      a_to buf a;
      b_to buf b;
      c_to buf c;
      d_to buf d
    let tuple5_to a_to b_to c_to d_to e_to buf (a, b, c, d, e) =
      a_to buf a;
      b_to buf b;
      c_to buf c;
      d_to buf d;
      e_to buf e

    let int32_to buf i =
      with_ buf 4
            (fun () -> set32_prim' buf.buf buf.pos i)

    let int64_to buf i =
      with_ buf 8
            (fun () -> set64_prim' buf.buf buf.pos i)

    let float_to buf f =
      int64_to buf (Int64.bits_of_float f)

    let int_to buf i = int32_to buf (Int32.of_int i)

    let char_to buf c =
      with_ buf 1
            (fun () -> Lwt_bytes.set buf.buf buf.pos c)

    let int8_to buf i =
      char_to buf (Char.chr i)

    let bool_to buf b =
      char_to buf (if b
                   then '\x01'
                   else '\x00')

    let option_to a_to buf = function
      | None -> bool_to buf false
      | Some a -> bool_to buf true;
                  a_to buf a

    let raw_substring_to buf (s, offset, length) =
      with_ buf length
            (fun () -> Lwt_bytes.blit_from_bytes s offset buf.buf buf.pos length)

    let raw_string_to buf s =
      raw_substring_to buf (s, 0, String.length s)

    let substring_to buf ((s, offset, length) as ss) =
      ensure_space buf (length + 4);
      int_to buf length;
      raw_substring_to buf ss

    let string_to buf s =
      substring_to buf (s, 0, String.length s)

    let bigstring_slice_to : Bigstring_slice.t serializer =
      fun buf bss ->
      let open Bigstring_slice in
      ensure_space buf (bss.length + 4);
      int_to buf bss.length;
      Lwt_bytes.blit bss.bs bss.offset buf.buf buf.pos bss.length;
      advance_pos buf bss.length

    let counted_list_to e_to buf (cnt, list) =
      int_to buf cnt;
      List.iter (e_to buf) (List.rev list)

    let counted_list_more_to a_to =
      pair_to
        (counted_list_to a_to)
        bool_to

    let list_to e_to buf list =
      (* TODO ideally the list should be iterated only once *)
      counted_list_to e_to buf (List.length list, list)

    let serialize_with_length ?(length = 20) a_to a =
      let buf = make ~length in
      int_to buf 0;
      a_to buf a;
      let len = buf.pos - 4 in
      set32_prim' buf.buf 0 (Int32.of_int len);
      buf

    let hashtbl_to ser_k ser_v buf h =
      let len = Hashtbl.length h in
      int_to buf len;
      Hashtbl.iter
        (fun k v ->
         ser_k buf k;
         ser_v buf v)
        h
  end

type 'a deserializer = 'a ReadBuffer.deserializer
type 'a serializer = 'a WriteBuffer.serializer

module Deser =
  struct
    type 'a t = 'a deserializer * 'a serializer

    let unit = ReadBuffer.unit_from, WriteBuffer.unit_to
    let bool = ReadBuffer.bool_from, WriteBuffer.bool_to
    let int = ReadBuffer.int_from, WriteBuffer.int_to
    let int32 = ReadBuffer.int32_from, WriteBuffer.int32_to
    let int64 = ReadBuffer.int64_from, WriteBuffer.int64_to
    let string = ReadBuffer.string_from, WriteBuffer.string_to
    let float = ReadBuffer.float_from, WriteBuffer.float_to

    let list (d, s) = ReadBuffer.list_from d, WriteBuffer.list_to s
    let counted_list (d, s) = ReadBuffer.counted_list_from d, WriteBuffer.counted_list_to s

    let tuple2 (d1, s1) (d2, s2) =
      ReadBuffer.pair_from d1 d2,
      WriteBuffer.pair_to s1 s2
    let pair = tuple2

    let tuple3 (d1,s1) (d2,s2) (d3,s3) =
      ReadBuffer.tuple3_from d1 d2 d3,
      WriteBuffer.tuple3_to s1 s2 s3
    let tuple4 (d1,s1) (d2,s2) (d3,s3) (d4,s4) =
      ReadBuffer.tuple4_from d1 d2 d3 d4,
      WriteBuffer.tuple4_to s1 s2 s3 s4
    let tuple5 (d1,s1) (d2,s2) (d3,s3) (d4,s4) (d5,s5) =
      ReadBuffer.tuple5_from d1 d2 d3 d4 d5,
      WriteBuffer.tuple5_to s1 s2 s3 s4 s5

    let option (d, s) = ReadBuffer.option_from d, WriteBuffer.option_to s
  end

module NetFdReader = struct
    open Lwt.Infix

    let with_lwt_bytes fd len f =
      let buf = Lwt_bytes.create len in
      Lwt.finalize
        (fun () -> Net_fd.read_all_lwt_bytes_exact fd buf 0 len >>= fun () ->
                   f buf)
        (fun () -> Lwt_bytes.unsafe_destroy buf;
                   Lwt.return_unit)

    let with_buffer_from fd len f =
      with_lwt_bytes
        fd len
        (fun buf ->
         let b = ReadBuffer.make_buffer buf 0 in
         f b)

    let from_buf fd len a_from =
      with_buffer_from
        fd len
        (fun buf -> a_from buf |> Lwt.return)

    let int32_from fd =
      from_buf fd 4 ReadBuffer.int32_from

    let int_from fd =
      from_buf fd 4 ReadBuffer.int_from

    let raw_string_from fd length =
      let s = Bytes.create length in
      Net_fd.read_all_exact fd s 0 length >>= fun () ->
      Lwt.return s

    let string_from fd =
      int_from fd >>= fun length ->
      raw_string_from fd length

    let char_from fd =
      from_buf fd 1 ReadBuffer.char_from

    let bool_from fd =
      char_from fd >>= function
      | '\x00' -> Lwt.return_false
      | '\x01' -> Lwt.return_true
      | k -> raise_bad_tag "bool from fd" (Char.code k)

    let option_from a_from fd =
      bool_from fd >>= function
      | false -> Lwt.return None
      | true -> a_from fd >>= fun a -> Lwt.return (Some a)
  end
