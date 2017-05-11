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

open! Prelude
open Nsm_model

let deser ~ser_version =
    let _from_buffer buf =
      let inflater =
        match Llio2.ReadBuffer.int8_from buf with
        | 1 -> Manifest.inner_from_buffer_1
        | 2 -> Manifest.inner_from_buffer_2
        | k -> Prelude.raise_bad_tag "Nsm_model.Manifest" k
      in
      let s = Snappy.uncompress (Llio2.ReadBuffer.string_from buf) in
      Prelude.deserialize inflater s
    in
    let _to_buffer buf t =
      Llio2.WriteBuffer.int8_to buf ser_version;
      let inner_to_buf =
        match ser_version with
        | 1 -> Manifest.inner_to_buffer_1
        | 2 -> Manifest.inner_to_buffer_2
        | k -> failwith "programmer error"
      in
      let res = Prelude.serialize inner_to_buf t in
      Llio2.WriteBuffer.string_to buf (Snappy.compress res)
    in
    _from_buffer, _to_buffer
