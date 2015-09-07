(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

open Prelude

module Compression = struct
  type t =
    | NoCompression
    | Snappy
    | Bzip2
  [@@deriving show]

  let output buf c =
    let t = match c with
      | NoCompression -> 1
      | Snappy -> 2
      | Bzip2 -> 3 in
    Llio.int8_to buf t

  let input buf =
    match Llio.int8_from buf with
    | 1 -> NoCompression
    | 2 -> Snappy
    | 3 -> Bzip2
    | k -> raise_bad_tag "Compression" k

end
