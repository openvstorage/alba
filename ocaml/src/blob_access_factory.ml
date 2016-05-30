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

open Blob_access

let make_directory_access files_path =
  let r = new default_directory_access files_path in
  (r :> directory_access)

let _init = ref false
let maybe_init_service gioexecfile =
  if not !_init
  then
    begin
      Gobjfs.IOExecFile.init gioexecfile;
      let align_size = 4096 in
      Gobjfs.GMemPool.init align_size;
      _init := true
      end

let make_directory_info
      ~engine
      ?(write_blobs = true)
                        files_path
                        ~use_fadvise
                        ~use_fallocate
  =
  let config = { write_blobs; files_path;use_fadvise;use_fallocate } in
  let open Asd_config in
  let () = Lwt_log.ign_info_f "engine:%s" (Config.show_blob_io_engine engine) in
  match engine with
  | Config.Pure -> (new directory_info config :> blob_dir_access)
  | Config.GioExecFile conf_file ->
     let () = maybe_init_service conf_file in
     (new Gblob_access.g_directory_info conf_file config :> blob_dir_access)

let endgame () =
  if !_init then Gobjfs.IOExecFile.destroy()
