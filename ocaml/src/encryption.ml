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

open Prelude

module Encryption = struct

  type key_length =
    | L256
  [@@deriving show]

  let key_length_to_buffer buf = function
    | L256 -> Llio.int8_to buf 1

  let key_length_from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> L256
    | k -> raise_bad_tag "Encryption.key_length" k

  type chaining_mode =
    | CBC
  [@@deriving show]

  let chaining_mode_to_buffer buf = function
    | CBC -> Llio.int8_to buf 1

  let chaining_mode_from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> CBC
    | k -> raise_bad_tag "Encryption.chaining_mode" k

  type algo =
    | AES of chaining_mode * key_length
  [@@deriving show]

  let algo_to_buffer buf = function
    | AES (mode, kl) ->
      Llio.int8_to buf 1;
      chaining_mode_to_buffer buf mode;
      key_length_to_buffer buf kl

  let algo_from_buffer buf =
    match Llio.int8_from buf with
    | 1 ->
      let mode = chaining_mode_from_buffer buf in
      let kl = key_length_from_buffer buf in
      AES (mode, kl)
    | k -> raise_bad_tag "Encryption.algo" k

  type key = HexString.t [@@deriving show]

  type t =
    | NoEncryption
    | AlgoWithKey of algo * key
  [@@deriving show]

  let to_buffer buf = function
    | NoEncryption ->
      Llio.int8_to buf 1
    | AlgoWithKey (algo, key) ->
      Llio.int8_to buf 2;
      algo_to_buffer buf algo;
      Llio.string_to buf key

  let from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> NoEncryption
    | 2 ->
      let algo = algo_from_buffer buf in
      let key = Llio.string_from buf in
      AlgoWithKey (algo, key)
    | k -> raise_bad_tag "Encryption.t" k

  let key_length = function
    | AES (_, L256) -> 256 / 8

  let is_valid = function
    | NoEncryption -> true
    | AlgoWithKey (algo, key) ->
      key_length algo = String.length key

  let verify_key_length algo key =
    assert (key_length algo = String.length key)

  let block_length = function
    | AES (_, _) -> 16
end
