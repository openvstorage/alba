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

open Lwt.Infix

let get_object_manifest'
      nsm_host_access
      manifest_cache
      ~namespace_id ~object_name
      ~consistent_read ~should_cache =
  Lwt_log.debug_f
    "get_object_manifest %li %S ~consistent_read:%b ~should_cache:%b"
    namespace_id object_name consistent_read should_cache
  >>= fun () ->
  let lookup_on_nsm_host namespace_id object_name =
    nsm_host_access # get_nsm_by_id ~namespace_id >>= fun client ->
    client # get_object_manifest_by_name object_name
  in
  Manifest_cache.ManifestCache.lookup
    manifest_cache
    namespace_id object_name
    lookup_on_nsm_host
    ~consistent_read ~should_cache
