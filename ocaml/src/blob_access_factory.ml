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

let _service_handle = ref None

let maybe_init_service gioexecfile =
  match !_service_handle with
  | None ->
    begin
      let service_handle = Gobjfs.IOExecFile.init gioexecfile in
      let align_size = 4096 in
      Gobjfs.GMemPool.init align_size;
      _service_handle := Some service_handle;
      service_handle
    end
  | Some sh -> sh

let make_directory_info
      ~engine
      ~statistics
      ?(write_blobs = true)
                        files_path
                        ~use_fadvise
                        ~use_fallocate
  =
  let config = { write_blobs; files_path;use_fadvise;use_fallocate } in
  let open Asd_config in
  let () = Lwt_log.ign_info_f "engine:%s" (Config.show_blob_io_engine engine) in
  match engine with
  | Config.Pure -> (new directory_info statistics config :> blob_dir_access)
  | Config.GioExecFile conf_file ->
     let service_handle = maybe_init_service conf_file in
     (new Gblob_access.g_directory_info statistics config service_handle :> blob_dir_access)

let endgame () =
  match !_service_handle with
  | None -> ()
  | Some sh -> Gobjfs.IOExecFile.destroy sh
