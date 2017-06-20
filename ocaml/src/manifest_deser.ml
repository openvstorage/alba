(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
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
