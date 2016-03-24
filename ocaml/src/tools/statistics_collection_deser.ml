(*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

open Statistics_collection.Generic

let to_buffer_with_version' ~ser_version buf t=
  let module Llio = Llio2.WriteBuffer in
  Llio.int8_to buf ser_version;
  Llio.float_to buf t.creation;
  Llio.float_to buf t.period;
  Llio.hashtbl_to Llio.int32_to Stat_deser.to_buffer' buf t.statistics

let from_buffer_raw' buf =
  let module Llio = Llio2.ReadBuffer in
  let creation = Llio.float_from buf in
  let period   = Llio.float_from buf in
  let statistics =
    Llio.hashtbl_from Llio.int32_from Stat_deser.from_buffer' buf
  in
  { creation; period; statistics}
