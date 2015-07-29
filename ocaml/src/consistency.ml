(*
Copyright 2015 Open vStorage NV

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

open Prelude
open Arakoon_client

type t = consistency =
  | Consistent
  | No_guarantees
  | At_least of Stamp.t

let from_buffer buf =
  match Llio.int8_from buf with
  | 0 -> Consistent
  | 1 -> No_guarantees
  | 2 -> At_least (Llio.int64_from buf)
  | k -> raise_bad_tag "Consistency" k

let to_buffer buf = function
  | Consistent -> Llio.int8_to buf 0
  | No_guarantees -> Llio.int8_to buf 1
  | At_least t ->
    Llio.int8_to buf 2;
    Llio.int64_to buf t
