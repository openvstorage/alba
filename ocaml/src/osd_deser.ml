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

module OsdInfo =
  struct
    open Nsm_model.OsdInfo

    let to_buffer buf t =
      let json = to_yojson t in
      let s = Yojson.Safe.to_string json in
      Llio2.WriteBuffer.string_to buf s
    let from_buffer buf =
      let s = Llio2.ReadBuffer.string_from buf in
      let json = Yojson.Safe.from_string s in
      match of_yojson json with
      | Result.Error s -> failwith s
      | Result.Ok t -> t

    let deser_json = from_buffer, to_buffer

    let deser =
      let from_buffer buf =
        let s = Llio2.ReadBuffer.string_from buf in
        Prelude.deserialize Nsm_model.OsdInfo.from_buffer s
      in
      let to_buffer buf t =
        let s =
          Prelude.serialize
            (Nsm_model.OsdInfo.to_buffer ~version:3) t
        in
        Llio2.WriteBuffer.string_to buf s
      in
        from_buffer, to_buffer
  end

module ClaimInfo =
  struct
    open Albamgr_protocol.Protocol.Osd.ClaimInfo

    let to_buffer buf =
      let module Llio = Llio2.WriteBuffer in
      function
      | ThisAlba osd_id ->
         Llio.int8_to buf 1;
         Llio.x_int64_to buf osd_id
      | AnotherAlba alba ->
         Llio.int8_to buf 2;
         Llio.string_to buf alba
      | Available ->
         Llio.int8_to buf 3

    let from_buffer buf =
      let module Llio = Llio2.ReadBuffer in
      match Llio.int8_from buf with
      | 1 -> ThisAlba (Llio.x_int64_from buf)
      | 2 -> AnotherAlba (Llio.string_from buf)
      | 3 -> Available
      | k -> raise_bad_tag "Osd.ClaimInfo" k

    let deser = from_buffer, to_buffer
  end
