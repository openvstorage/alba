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
type version = int [@@deriving show]

type location = Preset.osd_id option * version [@@deriving show]

type chunk_id = int [@@deriving show]
(* 0 <= fragment_id < k+m *)
type fragment_id = int [@@deriving show]

open Prelude

module Fragment = struct
  type t =
    { loc : location;
      crc : Checksum.t;
      len : int;
      ctr : string option } [@@deriving show]

  let make loc crc len ctr = {loc;crc;len;ctr}

  let loc_of x = x.loc
  let crc_of x = x.crc
  let len_of x = x.len
  let ctr_of x = x.ctr
  let osd_of x = x.loc |> fst

  let version_of x = x.loc |> snd

  let _inner_fragment_to buf t =
    Llio.int8_to buf 1;
    Llio.option_to x_int64_to buf (osd_of t);
    Llio.int_to buf (version_of t);

    Checksum.output buf t.crc;
    Llio.int_to buf t.len;
    Llio.option_to Llio.string_to buf t.ctr

  let _inner_fragment_from buf =

    let () =
      let tag = Llio.int8_from buf in
      match tag with
      | 1 -> ()
      | k -> raise_bad_tag "Fragment" k
    in
    let osd_id = Llio.option_from x_int64_from buf in
    let version = Llio.int_from buf in
    let loc = (osd_id, version) in
    let crc = Checksum.input buf in
    let len = Llio.int_from buf in
    let ctr = Llio.option_from Llio.string_from buf in
    make loc crc len ctr

  let fragment_to buf t =
    let s = serialize ~buf_size:512
                      _inner_fragment_to t
    in
    Llio.string_to buf s

  let fragment_from buf =
    let s = Llio.string_from buf in
    deserialize _inner_fragment_from s
end
