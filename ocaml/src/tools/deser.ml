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

type 'a t = 'a Llio.deserializer * 'a Llio.serializer

let serialize (_, serializer) = Prelude.serialize serializer
let deserialize (deserializer, _) = Prelude.deserialize deserializer

let from_buffer (deserializer, _) = deserializer
let to_buffer (_, serializer) = serializer


let unit = Llio.unit_from, Llio.unit_to
let int = Llio.int_from, Llio.int_to
let int64 = Llio.int64_from, Llio.int64_to
let string = Llio.string_from, Llio.string_to
let bool = Llio.bool_from, Llio.bool_to

let option (d,s) =
  Llio.option_from d, Llio.option_to s

let list (d,s) =
  Llio.list_from d, Llio.list_to s

let counted_list (d,s) =
  Llio.counted_list_from d, Llio.counted_list_to s

let pair (d1, s1) (d2, s2) =
  Llio.pair_from d1 d2,
  Llio.pair_to s1 s2

let tuple2 = pair
let tuple3 (d1,s1) (d2,s2) (d3,s3) =
  Llio.tuple3_from d1 d2 d3, Llio.tuple3_to s1 s2 s3
let tuple4 (d1,s1) (d2,s2) (d3,s3) (d4,s4) =
  Llio.tuple4_from d1 d2 d3 d4, Llio.tuple4_to s1 s2 s3 s4
let tuple5 (d1,s1) (d2,s2) (d3,s3) (d4,s4) (d5,s5) =
  Llio.tuple5_from d1 d2 d3 d4 d5, Llio.tuple5_to s1 s2 s3 s4 s5
let tuple6 (d1,s1) (d2,s2) (d3,s3) (d4,s4) (d5,s5) (d6,s6) =
  (fun buf ->
     let v1 = d1 buf in
     let v2 = d2 buf in
     let v3 = d3 buf in
     let v4 = d4 buf in
     let v5 = d5 buf in
     let v6 = d6 buf in
     (v1, v2, v3, v4, v5, v6)),
  (fun buf (v1, v2, v3, v4, v5, v6) ->
     s1 buf v1;
     s2 buf v2;
     s3 buf v3;
     s4 buf v4;
     s5 buf v5;
     s6 buf v6)

let versioned_to v ser buf x =
  Llio.pair_to Llio.int8_to ser buf (v,x)

let versioned_from v deser buf =
  let (v', r) = Llio.pair_from Llio.int8_from deser buf in
  assert (v' = v);
  r
