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

open Fragment
open Preset
open Checksum
open Prelude


module FragmentUpdate = struct
  module L = Alba_llio
  type t =
    { chunk_id: chunk_id ;
      fragment_id: fragment_id;
      osd_id_o: osd_id option;

      (* if the compressor version changes, fe *)
      size_change: (int * Checksum.t) option;
      ctr :bytes option;
      fnr :bytes option;
    }
      [@@deriving show]

  let make chunk_id fragment_id osd_id_o size_change ctr fnr =
    {chunk_id;fragment_id; osd_id_o; size_change; ctr; fnr}

  let from_buffer_v0 buf =
    let chunk_id     = L.int_from buf in
    let fragment_id  = L.int_from buf in
    let osd_id_o     = L.option_from x_int64_from buf in
    let size_change  = L.option_from
                         (L.pair_from
                            L.int_from
                            Checksum.from_buffer) buf in
    let ctr          = L.option_from L.string_from buf in
    make chunk_id fragment_id osd_id_o size_change ctr None

  let to_buffer_v0 buf t =
    L.int_to buf t.chunk_id;
    L.int_to buf t.fragment_id;
    L.option_to x_int64_to buf t.osd_id_o;
    L.option_to (L.pair_to L.int_to Checksum.to_buffer) buf t.size_change;
    L.option_to L.string_to buf t.ctr

  let _inner_to_buffer buf t =
    L.int8_to buf 1;
    L.int_to buf t.chunk_id;
    L.int_to buf t.fragment_id;
    L.option_to x_int64_to buf t.osd_id_o;
    L.option_to (L.pair_to L.int_to Checksum.to_buffer) buf t.size_change;
    L.option_to L.small_bytes_to buf t.ctr;
    L.option_to L.small_bytes_to buf t.fnr


  let _inner_from_buffer buf : t =
    let () =
      let tag = L.int8_from buf in
      match tag with
      | 1 -> ()
      | k -> raise_bad_tag "FragmentUpdate" k
    in
    let chunk_id      = L.int_from buf in
    let fragment_id   = L.int_from buf in
    let osd_id_o      = L.option_from x_int64_from buf in
    let size_change   = L.option_from
                          (L.pair_from
                             L.int_from
                             Checksum.from_buffer) buf
    in
    let ctr           = L.option_from L.small_bytes_from buf in
    let fnr           = L.option_from L.small_bytes_from buf in
    make chunk_id fragment_id osd_id_o size_change ctr fnr

  let to_buffer   buf t =
    Lwt_log.ign_debug_f "FragmentUpdate.to_buffer";
    let s = serialize ~buf_size:64 _inner_to_buffer t in
    L.small_bytes_to buf s

  let from_buffer buf  : t =
    let s = L.small_bytes_from buf in
    deserialize _inner_from_buffer s


end
