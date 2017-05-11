(*
Copyright (C) 2017 iNuron NV

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

open Checksum
type version = int [@@deriving show, yojson]

type chunk_id = int [@@deriving show]
(* 0 <= fragment_id < k+m *)
type fragment_id = int [@@deriving show]

module L = Alba_llio

module Fnr = struct
  type t = string option [@@deriving show, yojson]
  let to_buffer buf t =
    L.option_to L.small_bytes_to buf t

  let from_buffer buf = L.option_from L.small_bytes_from buf

  let maybe_from_buffer buf =
    Prelude.maybe_from_buffer from_buffer None buf
end

open! Prelude


module Fragment = struct
  type t =
    { osd : Preset.osd_id option;
      ver : version;
      crc : Checksum.t;
      len : int;
      ctr : string option;
      fnr : Fnr.t
    } [@@deriving show, yojson]

  let make  osd ver crc len ctr fnr = { osd; ver; crc; len; ctr; fnr}

  let make' (osd,ver) crc len ctr =
    { osd; ver; crc; len; ctr; fnr = None}

  let loc_of t = (t.osd, t.ver)
  let crc_of x = x.crc
  let len_of x = x.len
  let ctr_of x = x.ctr
  let osd_of x = x.osd
  let fnr_of x = x.fnr
  let ver_of x = x.ver

  let has_osd t = match osd_of t with
    | None   -> false
    | Some _ -> true


  let version_of x = x.ver

  let _inner_fragment_to buf t =
    L.int8_to buf 1;
    L.option_to x_int64_to buf t.osd;
    L.int_to buf t.ver;

    Checksum.output buf t.crc;
    L.int_to buf t.len;
    L.option_to L.small_bytes_to buf t.ctr;
    Fnr.to_buffer buf t.fnr

  let _inner_fragment_from buf =

    let () =
      let tag = L.int8_from buf in
      match tag with
      | 1 -> ()
      | k -> raise_bad_tag "Fragment" k
    in
    let osd = L.option_from x_int64_from buf in
    let ver = L.int_from buf in
    let crc = Checksum.input buf in
    let len = L.int_from buf in
    let ctr = L.option_from L.small_bytes_from buf in
    let fnr = Fnr.maybe_from_buffer buf in
    make osd ver crc len ctr fnr

  let fragment_to buf t =
    let s = serialize
              ~buf_size:64
              _inner_fragment_to t
    in
    L.small_bytes_to buf s

  let fragment_from buf =
    let s = L.small_bytes_from buf in
    deserialize _inner_fragment_from s
end
