(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

module Encryption = struct

  type key_length =
    | L256
  [@@deriving show, yojson]

  let key_length_to_buffer buf = function
    | L256 -> Llio.int8_to buf 1

  let key_length_from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> L256
    | k -> raise_bad_tag "Encryption.key_length" k

  let key_length = function
    | L256 -> 256 / 8

  type chaining_mode =
    | CBC
    | CTR
  [@@deriving show, yojson]

  let chaining_mode_to_buffer buf = function
    | CBC -> Llio.int8_to buf 1
    | CTR -> Llio.int8_to buf 2

  let chaining_mode_from_buffer buf =
    match Llio.int8_from buf with
    | 1 -> CBC
    | 2 -> CTR
    | k -> raise_bad_tag "Encryption.chaining_mode" k

  type algo =
    | AES of chaining_mode * key_length
  [@@deriving show, yojson]

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

  let algo_key_length = function
    | AES (_, key_len) -> key_length key_len

  let is_valid = function
    | NoEncryption -> true
    | AlgoWithKey (algo, key) ->
      algo_key_length algo = String.length key

  let verify_key_length algo key =
    assert (algo_key_length algo = String.length key)

  let block_length = function
    | AES (_, _) -> 16
end
