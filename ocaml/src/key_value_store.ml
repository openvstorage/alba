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

module type S = sig
  type t
  val show : t -> string
  val from_buffer : Llio.buffer -> t
  val to_buffer : Buffer.t -> t -> unit
  val pp : Format.formatter -> t -> unit
end


module Update_(S : S) = struct
  type key   = S.t [@@deriving show]
  type value = S.t [@@deriving show]
  type kvo = key * value option [@@deriving show]

  type transform =
    | ADD of int64 [@@deriving show]

  let apply_transform x = function
    | ADD y -> Int64.add x y

  type t =
   | Assert of kvo
   | Set of kvo
   | Transform of key * transform
  [@@deriving show]

  let assert' key value = Assert(key, Some value)
  let assert_none key = Assert(key, None)

  let set key value = Set(key, Some value)
  let delete key = Set(key, None)

  let compare_and_swap key expected value =
    [Assert(key, expected); Set(key, value)]

  let add key x64 = Transform (key, ADD x64)

  let from_buf buf =
    let kvo_from =
      Llio.pair_from
        S.from_buffer
        (Llio.option_from S.from_buffer)
    in
    match Llio.int8_from buf with
    | 1 -> Assert (kvo_from buf)
    | 2 -> Set (kvo_from buf)
    | 3 ->
       begin
         let key = S.from_buffer buf in
         let transform =
           match Llio.int8_from buf with
           | 1 -> let x = Llio.int64_from buf in ADD x
           | k -> raise_bad_tag "key_value_store ... transform" k
         in
         Transform (key, transform)
       end
    | k -> raise_bad_tag "key_value_store.Update.from_buf" k

  let to_buf buf =
    let kvo_to = Llio.pair_to S.to_buffer (Llio.option_to S.to_buffer) in
    function
    | Assert kvo ->
      Llio.int8_to buf 1;
      kvo_to buf kvo
    | Set kvo ->
      Llio.int8_to buf 2;
      kvo_to buf kvo
    | Transform (key, transform) ->
       Llio.int8_to buf 3;
       S.to_buffer buf key;
       begin
         match transform with
         | ADD x -> Llio.int8_to buf 1; Llio.int64_to buf x
       end
end
module Update = Update_(struct
    type t = string [@@deriving show]

    let from_buffer = Llio.string_from
    let to_buffer = Llio.string_to
  end)

exception InvalidCursor

type key   = string [@@deriving show]
type value = string [@@deriving show]
type kvo = key * value option [@@deriving show]

module type Read_key_value_store = sig
  type t

  val exists : t -> string -> bool
  val get : t -> string -> string option
  val get_exn : t -> string -> string

  type cursor

  val with_cursor : t -> (cursor -> 'a) -> 'a

  val cur_last : cursor -> bool
  val cur_get : cursor -> (key * value)
  val cur_get_key : cursor -> key
  val cur_get_value : cursor -> value
  val cur_prev : cursor -> bool
  val cur_next : cursor -> bool
  val cur_jump : cursor -> direction -> key -> bool
end

type expected = value option [@@ deriving show]
type actual = value option   [@@ deriving show]

type status =
  | Ok
  | AssertionFailed of (key * expected *  actual)
  | Ko of string
  [@@ deriving show]

(* don't let it propagate, it's supposed to be local *)
exception Status_exn of status

module type Key_value_store = sig
  include Read_key_value_store

  val apply_sequence : t -> ?comp:(value -> value -> int) -> Update.t list -> status
end

module Read_store_extensions(KV : Read_key_value_store) = struct

  let _fold_range cur comp_end_key cur_next max f init =
    let rec inner acc count =
      if count = max
      then begin
        let have_more = comp_end_key (KV.cur_get_key cur) in
        (count, acc), have_more
      end else
        begin
          let k = KV.cur_get_key cur in
          if comp_end_key k
          then
            begin
              let count' = count + 1 in
              let acc' = f cur k count acc in
              if cur_next cur
              then
                inner acc' count'
              else
                (count', acc'), false
            end
          else
            (count, acc), false
        end
    in
    inner init 0

  let fold_range t ~first ~finc ~last ~max ~reverse f init =
    KV.with_cursor
      t
      (fun cur ->
         if reverse
         then begin
           let jump_last = match last with
             | None -> KV.cur_last cur
             | Some (last, linc) ->
               KV.cur_jump cur Left last &&
               (linc || (last <> KV.cur_get_key cur) || KV.cur_prev cur) in
           if jump_last
           then begin
             let comp_first_key =
               if finc
               then fun k -> String.(k >=: first)
               else fun k -> String.(k >: first) in
             _fold_range cur comp_first_key KV.cur_prev max f init
           end else
             (0, init), false
         end else begin
           if KV.cur_jump cur Right first &&
              (finc || (first <> KV.cur_get_key cur) || KV.cur_next cur)
           then begin
             let comp_last_key =
               match last with
               | None -> fun _ -> true
               | Some (last, linc) ->
                 if linc
                 then fun k -> String.(k <=: last)
                 else fun k -> String.(k <: last) in
             _fold_range cur comp_last_key KV.cur_next max f init
           end else
             (0, init), false
         end)

  let map_range t ~first ~finc ~last ~max ~reverse f =
    let (cnt, items), have_more =
      fold_range
        t
        ~first ~finc ~last
        ~max ~reverse
        (fun cur key _cnt acc ->
           let item = f cur key in
           item :: acc)
        []
    in
    (cnt, List.rev items), have_more

  let range t ~first ~finc ~last ~max ~reverse =
    map_range
      t
      ~first ~finc ~last
      ~max ~reverse
      (fun cur key -> key)
end

module Store_extensions(KV : Key_value_store) = struct
  module E = Read_store_extensions(KV)
  include E

  let set t k v =
    (KV.apply_sequence t [Update.set k v])

end

let next_prefix prefix =
  if 0 = String.length prefix
  then
    None
  else
    let next_char c =
      let code = Char.code c + 1 in
      match code with
      | 256 -> Char.chr 0, true
      | code -> Char.chr code, false in
    let rec inner s pos =
      let c, carry = next_char (Bytes.get s pos) in
      Bytes.set s pos c;
      match carry, pos with
      | false, _ -> Some (Bytes.unsafe_to_string s, false)
      | true, 0 -> None
      | true, pos -> inner s (pos - 1) in
    let copy = Bytes.of_string prefix in
    inner copy ((Bytes.length copy) - 1)
