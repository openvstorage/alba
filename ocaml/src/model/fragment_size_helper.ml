(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

(* this should be at least sizeof(long) according to the jerasure documentation *)
let fragment_multiple = 16

let round_down_to_multiple x y =
  (x / y) * y

let round_up_to_multiple x y =
  let r = x mod y in
  if r = 0
  then x
  else x - r + y

let determine_chunk_size ~object_length ~max_fragment_size ~k =
  if k = 1
  then
    (* replication doesn't have to take fragment_multiple in to account *)
    let max_chunk_size = max_fragment_size * k in (* max_chunk_size = max_fragment_size *)
    min object_length max_chunk_size
  else
    begin
      let erasure_coding_max_fragment_size = round_down_to_multiple max_fragment_size fragment_multiple in
      assert (erasure_coding_max_fragment_size > 0);
      let max_chunk_size = erasure_coding_max_fragment_size * k in
      if max_chunk_size < object_length
      then max_chunk_size
      else round_up_to_multiple object_length (k * fragment_multiple)
    end

let old_desired_chunk_size ~object_length ~max_fragment_size ~k =
  let x = fragment_multiple * k in
  if max_fragment_size > (object_length / k)
  then ((object_length / x) + 1) * x
  else (max_fragment_size / fragment_multiple) * x
