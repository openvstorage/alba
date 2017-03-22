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

module Statistics = struct

  let section = Lwt_log.Section.make "statistics"

  type duration = float [@@deriving show]

  type fragment_upload = {
    osd_id_o : int64 option;
    size_orig : int;
    size_final : int;
    compress_encrypt : duration;
    hash : duration;
    store_osd : duration;
    total : duration;
  } [@@deriving show]

  type chunk_upload = {
    read_data : duration;
    fragments : fragment_upload list;
    total : duration;
  } [@@deriving show]

  type object_upload = {
    size : int;
    hash : duration;
    chunks : chunk_upload list;
    store_manifest : duration;
    total : duration;
  } [@@deriving show]


  type fragment_download = {
    osd_id : int64;
    retrieve : duration;
    verify : duration;
    decrypt : duration;
    decompress : duration;
    total : duration;
  } [@@deriving show]

  type deduplicated = bool [@@deriving show]

  type fragment_fetch =
    | FromCache of duration
    | FromOsd of (fragment_download * deduplicated)
  [@@deriving show]

  type chunk_download = {
    fragments : fragment_fetch list;
    gather_decode : duration;
    total : duration;
  } [@@deriving show]



  type object_download = {
    get_manifest_dh : duration * Cache.value_source;
    chunks : chunk_download list;
    verify : duration;
    write_data : duration;
    total : duration;
  } [@@deriving show]

  let summed_fragment_hit_misses (object_download:object_download) =
    List.fold_left
      (fun (h,m) (chunk_download:chunk_download) ->
       List.fold_left
         (fun (h,m) (fragment_download:fragment_fetch)->
          match fragment_download with
          | FromCache _ -> (h+1,m)
          | FromOsd _   -> (h,m+1)
         ) (h,m) chunk_download.fragments
      ) (0,0) object_download.chunks
end
