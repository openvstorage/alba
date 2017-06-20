(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Key_value_store

module Rocks_key_value_store_ = struct
  type t = Rocks.t

  open Rocks

  let create' ?max_open_files ?recycle_log_file_num ?block_cache_size ~db_path () =
    let opts = Options.create () in
    Options.set_create_if_missing opts true;
    Options.set_use_fsync opts true;
    Option.iter (Options.set_max_open_files opts) max_open_files;
    Option.iter (Options.set_recycle_log_file_num opts) recycle_log_file_num;
    let () = match block_cache_size with
      | None -> ()
      | Some capacity ->
         let cache = Cache.create_no_gc capacity in
         let bbto = BlockBasedTableOptions.create () in
         BlockBasedTableOptions.set_block_cache bbto cache;
         Options.set_block_based_table_factory opts bbto;
         Cache.destroy cache
    in
    Rocks.open_db ~opts db_path

  let create () =
    create' ~db_path:(string_of_int (Random.int 1_000_000))

  let get t key = get_string t key
  let get_exn t key = Option.get_some (get t key)
  let exists t key = get t key <> None

  type cursor = Iterator.t

  let with_cursor t f = Iterator.with_t t ~f

  let with_valid f c =
    f c;
    Iterator.is_valid c

  let cur_last = with_valid (fun c -> Iterator.seek_to_last c)

  let cur_get_key = Iterator.get_key_string
  let cur_get_value = Iterator.get_value_string
  let cur_get c = cur_get_key c, cur_get_value c

  let cur_prev = with_valid (fun c -> Iterator.prev c)
  let cur_next = with_valid (fun c -> Iterator.next c)
  let cur_jump c dir key =
    Iterator.seek_string c key;
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
         WriteBatch.put_string wb k v;
         WriteOptions.with_t
           (fun opts ->
              WriteOptions.set_sync opts true;
              write ~opts t wb))

end

module Rocks_key_value_store = struct
  include Rocks_key_value_store_

  module Ext = Read_store_extensions(Rocks_key_value_store_)
  include Ext
end
