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

type location = Preset.osd_id option * version [@@deriving show, yojson]

type chunk_id = int [@@deriving show]
(* 0 <= fragment_id < k+m *)
type fragment_id = int [@@deriving show]

open Prelude

module L = Alba_llio
module Fragment = struct
  type t =
    { loc : location;
      crc : Checksum.t;
      len : int;
      ctr : string option;
      fnr : string option
    } [@@deriving show, yojson]

  let make  loc crc len ctr fnr = {loc;crc;len;ctr; fnr}
  let make' loc crc len ctr = { loc; crc;len; ctr; fnr = None}

  let loc_of x = x.loc
  let crc_of x = x.crc
  let len_of x = x.len
  let ctr_of x = x.ctr
  let osd_of x = x.loc |> fst
  let fnr_of x = x.fnr

  let has_loc t = match osd_of t with
    | None   -> false
    | Some _ -> true

  let version_of x = x.loc |> snd

  let _inner_fragment_to buf t =
    L.int8_to buf 1;
    L.option_to x_int64_to buf (osd_of t);
    L.int_to buf (version_of t);

    Checksum.output buf t.crc;
    L.int_to buf t.len;
    L.option_to L.small_bytes_to buf t.ctr;
    L.option_to L.small_bytes_to buf t.fnr

  let _inner_fragment_from buf =

    let () =
      let tag = L.int8_from buf in
      match tag with
      | 1 -> ()
      | k -> raise_bad_tag "Fragment" k
    in
    let osd_id = L.option_from x_int64_from buf in
    let version = L.int_from buf in
    let loc = (osd_id, version) in
    let crc = Checksum.input buf in
    let len = L.int_from buf in
    let ctr = L.option_from L.small_bytes_from buf in
    let fnr = maybe_from_buffer (L.option_from L.small_bytes_from) None buf in
    make loc crc len ctr fnr

  let fragment_to buf t =
    let s = serialize
              ~buf_size:512
              _inner_fragment_to t
    in
    L.small_bytes_to buf s

  let fragment_from buf =
    let s = L.small_bytes_from buf in
    deserialize _inner_fragment_from s
end
