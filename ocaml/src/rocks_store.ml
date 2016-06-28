(*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*)

open Prelude
open Key_value_store
open Rocks

module Rocks_key_value_store_ = struct
  type t = RocksDb.t

  let create' ?max_open_files ?recycle_log_file_num ?block_cache_size ~db_path () =
    let options = Options.create_gc () in
    Options.set_create_if_missing options true;
    Options.set_use_fsync options true;
    Option.iter (Options.set_max_open_files options) max_open_files;
    Option.iter (Options.set_recycle_log_file_num options) recycle_log_file_num;
    let () = match block_cache_size with
      | None -> ()
      | Some capacity ->
         let cache = Cache.create_no_gc capacity in
         let bbto = BlockBasedTableOptions.create_gc () in
         BlockBasedTableOptions.set_block_cache bbto cache;
         Options.set_block_based_table_factory options bbto;
         Cache.destroy cache
    in
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
