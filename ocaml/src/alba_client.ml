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

open Lwt.Infix
open Fragment_cache
open Albamgr_access
open Alba_base_client
open Alba_statistics
open Alba_client_errors

class alba_client (fragment_cache : cache)
                  ~(mgr_access : Albamgr_client.client)
                  ~manifest_cache_size
                  ~bad_fragment_callback
                  ~nsm_host_connection_pool_size
                  ~osd_connection_pool_size
  = object(self)
  inherit alba_base_client
            fragment_cache
            ~mgr_access
            ~manifest_cache_size
            ~bad_fragment_callback
            ~nsm_host_connection_pool_size
            ~osd_connection_pool_size

  method get_object_manifest ~namespace ~object_name ~consistent_read ~should_cache =
      self # nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->
           self # get_object_manifest'
             ~namespace_id ~object_name
             ~consistent_read ~should_cache)


    method download_object_slices_to_string
      ~namespace
      ~object_name
      ~object_slices
      ~consistent_read
      =
      let length =
        List.fold_left
          (fun acc (offset, len) -> acc + len)
          0
          object_slices
      in
      let dest = Bytes.create length in
      self # download_object_slices
        ~namespace
        ~object_name
        ~object_slices
        ~consistent_read
        (fun dest_off src off len ->
           Lwt_log.debug_f "Writing %i bytes at offset %i" len dest_off >>= fun () ->
           Lwt_bytes.blit_to_bytes src off dest dest_off len;
           Lwt.return_unit) >>= function
      | None -> Lwt.return None
      | Some _ -> Lwt.return (Some dest)

  method download_object_generic
           ~namespace
           ~object_name
           ~consistent_read
           ~should_cache
           ~(write_object_data :
               Int64.t ->
               ((char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t ->
                int ->
                int ->
                unit Lwt.t) Lwt.t)
         : (Nsm_model.Manifest.t *
            Statistics.object_download)option Lwt.t

    =
    self # nsm_host_access # with_namespace_id
      ~namespace
      (fun namespace_id ->
         self # download_object_generic'
           ~namespace_id
           ~object_name
           ~write_object_data
           ~consistent_read
           ~should_cache)

  method download_object_to_file
           ~namespace
           ~object_name
           ~(output_file : string)
           ~consistent_read
           ~should_cache
    =
    Lwt_log.debug_f
      "Downloading object %S (namespace=%S) ~consistent_read:%b to file %s"
      object_name
      namespace
      consistent_read
      output_file >>= fun () ->

    let fd_ref = ref None in
    let write_object_data total_size =
      Lwt_unix.openfile
        output_file
        Lwt_unix.([O_WRONLY; O_CREAT;])
        0o664 >>= fun fd ->
      fd_ref := Some fd;
      Lwt.return (Lwt_extra2.write_all_lwt_bytes fd)
    in
    Lwt.finalize
      (fun () ->
       self # download_object_generic
            ~namespace
            ~object_name
            ~write_object_data
            ~consistent_read
            ~should_cache
      )
      (fun () ->
       match !fd_ref with
       | None -> Lwt.return ()
       | Some fd ->
          Lwt_unix.fsync fd >>= fun () ->
          Lwt_unix.close fd)


  method download_object_to_string
           ~namespace
           ~object_name
           ~consistent_read
           ~should_cache
         : string option Lwt.t =
    let res = ref None in
    let write_object_data total_size =
      let bs = Bytes.create (Int64.to_int total_size) in
      res := Some bs;
      let offset = ref 0 in
      let write source pos len =
        Lwt_bytes.blit_to_bytes source pos bs !offset len;
        offset := !offset + len;
        Lwt.return ()
      in
      Lwt.return write
    in
    self # download_object_generic
         ~namespace
         ~object_name
         ~write_object_data
         ~consistent_read
         ~should_cache
    >>= fun _len ->
    Lwt.return !res

  method download_object_to_string'
           ~namespace_id
           ~object_name
           ~consistent_read
           ~should_cache
         : string option Lwt.t =
    let res = ref None in
    let write_object_data total_size =
      let bs = Bytes.create (Int64.to_int total_size) in
      res := Some bs;
      let offset = ref 0 in
      let write source pos len =
        Lwt_bytes.blit_to_bytes source pos bs !offset len;
        offset := !offset + len;
        Lwt.return ()
      in
      Lwt.return write
    in
    self # download_object_generic'
         ~namespace_id
         ~object_name
         ~write_object_data
         ~consistent_read
         ~should_cache
    >>= fun _len ->
    Lwt.return !res

  method delete_object ~namespace ~object_name ~may_not_exist =
    Lwt_log.debug_f "Deleting object %S (namespace=%S)"
                    object_name namespace >>= fun () ->
    self # nsm_host_access # with_namespace_id
      ~namespace
      (fun namespace_id ->
         self # nsm_host_access # get_nsm_by_id ~namespace_id >>= fun client ->
         Lwt.finalize
           (fun () ->self # delete_object' client ~object_name ~may_not_exist)
           (fun () ->
              let () =
                Manifest_cache.ManifestCache.remove
                  _mf_cache namespace_id object_name
              in
              Lwt.return_unit
           ))

  method download_object_generic'
             ~namespace_id
             ~object_name
             ~consistent_read
             ~should_cache
             ~(write_object_data :
                 Int64.t ->
               (Lwt_bytes.t -> int -> int -> unit Lwt.t) Lwt.t)
           : (Nsm_model.Manifest.t *
              Statistics.object_download) option Lwt.t

      =
      let t0_object = Unix.gettimeofday () in
      Statistics.with_timing_lwt
        (fun () -> self # get_object_manifest'
                        ~namespace_id ~object_name
                        ~consistent_read ~should_cache)
      >>= fun r ->
      match r with
      | _, (hit_or_miss, None) -> Lwt.return None
      | (t_get_manifest, (hit_or_miss, Some manifest)) ->
        let open Nsm_model in
        Lwt_log.debug_f
          "download_object: found %s, hit_or_miss=%b manifest = %s"
          object_name
          hit_or_miss
          (Manifest.show manifest) >>= fun () ->

        write_object_data manifest.Manifest.size >>= fun write_object_data ->
        let get_manifest_dh = t_get_manifest, hm_to_source hit_or_miss in
        let attempt_download get_manifest_dh manifest =
          self # download_object_generic''
               ~namespace_id
               ~manifest
               ~get_manifest_dh
               ~t0_object
               ~write_object_data
        in
        begin
          Lwt.catch
            (fun () -> attempt_download get_manifest_dh manifest)
            (function
              | Error.Exn Error.NotEnoughFragments as exn ->
                 (* Download probably failed because of stale manifest *)
                 Lwt_log.info_f ~exn "retrying " >>= fun () ->
                 Statistics.with_timing_lwt
                   (fun () ->
                    self # get_object_manifest'
                         ~namespace_id
                         ~object_name
                         ~consistent_read:true
                         ~should_cache)
                 >>= fun (delay, (_, fresh_o))  ->
                 begin
                   match fresh_o with
                   | None -> Lwt.return None
                   | Some fresh ->
                      let get_manifest_dh' = (delay, Statistics.Stale) in
                      attempt_download get_manifest_dh' fresh
                 end
              | exn -> Lwt.fail exn
            )
        end

    method list_objects ~namespace ~first ~finc ~last ~max ~reverse =
      self # nsm_host_access # with_nsm_client
        ~namespace
        (fun client ->
           client # list_objects
             ~first ~finc ~last
             ~max ~reverse)

    method invalidate_cache (namespace:string) : unit Lwt.t =
      Lwt_log.debug_f "invalidate_cache:%S" namespace >>= fun () ->
      self # nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->
           (* Fragment cache needs no invalidation as keys are globally unique *)
           let open Manifest_cache in
           ManifestCache.invalidate _mf_cache namespace_id)

    method drop_cache_by_id namespace_id =
      Manifest_cache.ManifestCache.drop _mf_cache namespace_id;
      fragment_cache # drop namespace_id

    method drop_cache namespace =
      self # nsm_host_access # with_namespace_id
        ~namespace
        (self # drop_cache_by_id)

    method delete_namespace ~namespace =
      Lwt_log.debug_f "Alba_client: delete_namespace %S" namespace >>= fun () ->

      self # nsm_host_access # with_namespace_id
        ~namespace
        (fun namespace_id ->

           Lwt.ignore_result begin
             Lwt_extra2.ignore_errors
               (fun () -> self # drop_cache_by_id namespace_id)
           end;

           mgr_access # list_all_namespace_osds ~namespace_id >>= fun (_, osds) ->

           mgr_access # delete_namespace ~namespace >>= fun nsm_host_id ->

           self # deliver_nsm_host_messages ~nsm_host_id)
  end

let with_client albamgr_client_cfg
                ?cache_dir
                ?(manifest_cache_size=100_000)
                ?(fragment_cache_size=100_000_000)
                ?(rocksdb_max_open_files=256)
                ?(bad_fragment_callback = fun
                    alba_client
                    ~namespace_id ~object_id ~object_name
                    ~chunk_id ~fragment_id ~version_id -> ())
                ?(albamgr_connection_pool_size = 10)
                ?(nsm_host_connection_pool_size = 10)
                ?(osd_connection_pool_size = 10)
                f
  =
  begin
    match cache_dir with
    | Some cd ->
       let max_size = Int64.of_int fragment_cache_size in
       safe_create cd ~max_size ~rocksdb_max_open_files
       >>= fun cache ->
       Lwt.return (cache :> cache)
    | None ->
       let cache = new no_cache in
       Lwt.return (cache :> cache)
  end
  >>= fun cache ->
  let mgr_access =
    new Albamgr_client.client
      (new basic_mgr_pooled
        ~albamgr_connection_pool_size
        ~albamgr_client_cfg
        default_buffer_pool)
  in
  let client = new alba_client
                   cache
                   ~mgr_access
                   ~manifest_cache_size
                   ~bad_fragment_callback
                   ~nsm_host_connection_pool_size
                   ~osd_connection_pool_size
  in
  f client
