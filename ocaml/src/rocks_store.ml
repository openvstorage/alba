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
open Key_value_store
open Rocks

module Rocks_key_value_store_ = struct
  type t = RocksDb.t

  let create' ?max_open_files ~db_path () =
    let options = Options.create_gc () in
    Options.set_create_if_missing options true;
    Options.set_use_fsync options true;
    Option.iter (Options.set_max_open_files options) max_open_files;
    RocksDb.open_db options db_path

  let create () =
    create' ~db_path:(string_of_int (Random.int 1_000_000))

  let get t key =
    ReadOptions.with_t
      (fun ro -> RocksDb.get t ro key)
  let get_exn t key = Option.get_some (get t key)
  let exists t key = get t key <> None

  type cursor = Iterator.t

  let with_cursor t f =
    ReadOptions.with_t
      (fun ro -> Iterator.with_t t ro f)

  let with_valid f c =
    f c;
    Iterator.is_valid c

  let cur_last = with_valid (fun c -> Iterator.seek_to_last c)

  let cur_get_key = Iterator.get_key
  let cur_get_value = Iterator.get_value
  let cur_get c = cur_get_key c, cur_get_value c

  let cur_prev = with_valid (fun c -> Iterator.prev c)
  let cur_next = with_valid (fun c -> Iterator.next c)
  let cur_jump c dir key =
    Iterator.seek c key;
    if Iterator.is_valid c
    then begin
      match dir with
      | Right -> true
      | Left -> begin
          (* maybe we jumped to far... *)
          if cur_get_key c = key
          then true
          else cur_prev c
        end
    end else begin
      match dir with
      | Right -> false
      | Left -> cur_last c
    end

  let set t k v =
    WriteBatch.with_t
      (fun wb ->
         WriteBatch.put wb k v;
         WriteOptions.with_t
           (fun wo ->
              WriteOptions.set_sync wo true;
              RocksDb.write t wo wb))

end

module Rocks_key_value_store = struct
  include Rocks_key_value_store_

  module Ext = Read_store_extensions(Rocks_key_value_store_)
  include Ext
end
