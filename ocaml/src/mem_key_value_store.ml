(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Key_value_store

module Mem_key_value_store_ = struct
  module StringMap = Map.Make(String)
  type t = { mutable map : string StringMap.t }

  let create () = { map = StringMap.empty }

  let exists t key = StringMap.mem key t.map
  let get t key =
    try Some(StringMap.find key t.map)
    with Not_found -> None
  let get_exn t key = StringMap.find key t.map

  let apply_sequence t ?(comp=Pervasives.compare) upds : status =
    try
      t.map <-
        List.fold_left
          (fun kv upd ->
             match upd with
             | Update.Assert(key, Some value) ->
                begin
                  if not (StringMap.mem key t.map)
                  then
                    let status = AssertionFailed(key, Some value, None) in
                    raise (Status_exn status)
                  else
                    let mvalue = StringMap.find key t.map in
                    if mvalue = value
                    then kv
                    else
                      let status = AssertionFailed(key, Some value,Some mvalue) in
                      raise (Status_exn status)
                end
             | Update.Assert(key, None) ->
                begin
                  if not (StringMap.mem key t.map)
                  then kv
                  else
                    let mvalue = StringMap.find key t.map in
                    let status = AssertionFailed(key, None, Some mvalue) in
                    raise (Status_exn status)
                end
             | Update.Set(key, Some value) -> StringMap.add key value kv
             | Update.Set(key, None) -> StringMap.remove key kv
             | Update.Transform(key,tr) ->
                begin
                  let old_value =
                    try
                      let v = StringMap.find key t.map in
                      let bv = Llio.make_buffer v 0 in
                      Llio.int64_from bv
                    with Not_found -> 0L
                  in
                  let new_value =
                    let new64 = Update.apply_transform old_value tr in
                    serialize Llio.int64_to new64
                  in
                  StringMap.add key new_value kv
                end
          )

          t.map
          upds;
      Ok
    with
    | Status_exn x -> x
    | e -> Ko (Printexc.to_string e)

  module Zipper = StringMap.Zipper
  type cursor = { t : t;
                  mutable zip : string Zipper.t option; }

  let with_cursor t f = f { t; zip = None; }

  let cur_is_valid cur = match cur.zip with
    | None -> false
    | Some _ -> true

  let _get_zipper cur = match cur.zip with
    | None -> raise InvalidCursor
    | Some z -> z

  let _new_zipper cur z =
    cur.zip <- z;
    cur_is_valid cur

  let _update_zipper cur f =
    _new_zipper cur (f (_get_zipper cur))

  let cur_jump cur dir key = _new_zipper cur (Zipper.jump ~dir key cur.t.map)
  let cur_last cur = _new_zipper cur (Zipper.last cur.t.map)
  let cur_next cur = _update_zipper cur (fun z -> Zipper.next z)
  let cur_prev cur = _update_zipper cur (fun z -> Zipper.prev z)

  let cur_get cur = Zipper.get (_get_zipper cur)
  let cur_get_key cur = fst (cur_get cur)
  let cur_get_value cur = snd (cur_get cur)
end

module Mem_key_value_store = struct
  include Mem_key_value_store_

  module Ext = Store_extensions(Mem_key_value_store_)
  include Ext
end
