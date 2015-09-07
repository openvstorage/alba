(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

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
    hit_or_miss: bool;
    verify : duration;
    decrypt : duration;
    decompress : duration;
    total : duration;
  } [@@deriving show]

  type chunk_download = {
    fragments : fragment_download list;
    gather_decode : duration;
    total : duration;
  } [@@deriving show]

  type manifest_source =
    | Cache
    | NsmHost
    | Stale
    [@@ deriving show]

  type object_download = {
    get_manifest_dh : duration * manifest_source;
    chunks : chunk_download list;
    verify : duration;
    write_data : duration;
    total : duration;
  } [@@deriving show]

  let with_timing f =
    let t0 = Unix.gettimeofday () in
    let res = f () in
    let t1 = Unix.gettimeofday () in
    t1 -. t0, res

  let with_timing_lwt f =
    let t0 = Unix.gettimeofday () in
    let open Lwt.Infix in
    f () >>= fun res ->
    let t1 = Unix.gettimeofday () in
    Lwt.return (t1 -. t0, res)

  let summed_fragment_hit_misses (object_download:object_download) =
    List.fold_left
      (fun (h,m) (chunk_download:chunk_download) ->
       List.fold_left
           (fun (h,m) (fragment_download:fragment_download)->
            if fragment_download.hit_or_miss
            then (h+1,m)
            else (h,m+1)
           ) (h,m) chunk_download.fragments
      ) (0,0) object_download.chunks
end
