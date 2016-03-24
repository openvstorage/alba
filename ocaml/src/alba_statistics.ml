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

module Statistics = struct

  let section = Lwt_log.Section.make "statistics"

  type duration = float [@@deriving show]

  type fragment_upload = {
    osd_id_o : int32 option;
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
    osd_id : int32;
    retrieve : duration;
    verify : duration;
    decrypt : duration;
    decompress : duration;
    total : duration;
  } [@@deriving show]

  type fragment_fetch =
    | FromCache of duration
    | FromOsd of fragment_download
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
