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
open Registry

module type ReadUserDb = sig
  val prefix : string
  val db : read_user_db
end
let key_has_prefix prefix key =
  let prefix_len = String.length prefix in
  if Key.length key < prefix_len
  then false
  else begin
    let raw_key = Key.get_raw key in
    let rec inner pos =
      if pos = prefix_len
      then true
      else begin
        if raw_key.[pos+1] = prefix.[pos]
        then inner (pos + 1)
        else false
      end in
    inner 0
  end

module WrapReadUserDb(RUD : ReadUserDb) = struct
  type t = read_user_db

  let prefix = RUD.prefix
  let prefix_len = String.length prefix
  let prefix_wrap_string k = prefix ^ k
  let prefix_unwrap_string k = Str.string_after k prefix_len
  let prefix_unwrap_key k = Key.sub k prefix_len (Key.length k - prefix_len)
  let key_has_prefix k = key_has_prefix prefix k

  let exists (t:t) key = match t # get (prefix_wrap_string key) with Some _ -> true | None -> false
  let get t key = t # get (prefix_wrap_string key)
  let get_exn t key = match t # get (prefix_wrap_string key)
    with
    | Some v -> v
    | None -> failwith (Printf.sprintf "key %s not found" key)

  type cursor =  { cur : cursor_db;
                   mutable key : string option; }

  let with_cursor (t:t) f =
    t # with_cursor
      (fun cur ->
         f { cur ; key = None; })

  let assert_key_valid c =
    if c.key = None
    then failwith "invalid cursor"
  let jump_and_validate c f =
    if f ()
    then begin
      let k = c.cur # get_key () in
      if key_has_prefix k
      then begin
        c.key <- Some (prefix_unwrap_key k);
        true
      end else begin
        c.key <- None;
        false
      end
    end else begin
      c.key <- None;
      false
    end

  let cur_get_key cur =
    match cur.key with
    | None -> failwith "invalid cursor"
    | Some k -> k
  let cur_get_value cur =
    assert_key_valid cur;
    cur.cur # get_value ()
  let cur_get (cur:cursor) = cur_get_key cur, cur_get_value cur

  let cur_last cur =
    match Key_value_store.next_prefix prefix with
    | None ->
      jump_and_validate
        cur
        (fun () ->
           cur.cur # last ())
    | Some (prefix_next, _) ->
      jump_and_validate
        cur
        (fun () ->
           cur.cur # jump
             ~inc:false
             ~right:false
             prefix_next)
  let cur_prev cur = jump_and_validate cur (fun () -> cur.cur # prev ())
  let cur_next cur = jump_and_validate cur (fun () -> cur.cur # next ())
  let cur_jump cur dir key =
    jump_and_validate
      cur
      (fun () ->
         cur.cur # jump
           ~inc:true
           ~right:(match dir with | Right -> true | Left -> false)
           (prefix_wrap_string key))
end

let apply_update (db : user_db) =
  let open Update.Update in
  function
  | Replace (k, None)
  | Delete k ->
     db # put k None
  | Replace (k, Some v)
  | Set (k, v) ->
     db # put k (Some v)
  | Assert_range _
  | Assert_exists _
  | Assert _ ->
     (* all asserts should be noops when executed immediatelly *)
     ()
  | UserFunction (name, value_o) ->
     let _ : string option = Registry.lookup name db value_o in
     ()
  | Nop -> ()
  | DeletePrefix _
  | SyncedSequence _
  | AdminSet _
  | SetRoutingDelta _
  | SetRouting _
  | SetInterval _
  | Sequence _
  | TestAndSet _
  | MasterSet _ ->
     (* these are not (yet?) supported here *)
     assert false
