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
open Fragment_cache
open Albamgr_access
open Alba_base_client
open Alba_statistics
open Alba_client_errors

class alba_client (base_client : Alba_base_client.client)
  =
  let mgr_access = base_client # mgr_access in
  let nsm_host_access = base_client # nsm_host_access in
  let osd_access = base_client # osd_access in
  let fragment_cache = base_client # get_fragment_cache in

  let deliver_nsm_host_messages ~nsm_host_id =
    Alba_client_message_delivery.deliver_nsm_host_messages
      mgr_access nsm_host_access osd_access
      ~nsm_host_id
  in

  object(self)
    method get_base_client = base_client
    method mgr_access = mgr_access
    method nsm_host_access = nsm_host_access
    method osd_access = osd_access

    method get_object_manifest' = base_client # get_object_manifest'
    method download_object_slices = base_client # download_object_slices
    method download_object_generic'' = base_client # download_object_generic''

    method create_namespace ~namespace ~preset_name ?nsm_host_id () =
      Alba_client_namespace.create_namespace
        mgr_access
        (base_client # get_preset_info)
        (base_client # deliver_messages_to_most_osds)
        deliver_nsm_host_messages
        ~namespace ~preset_name ?nsm_host_id ()

    method upload_object_from_file = base_client # upload_object_from_file

    method discover_osds_check_claimed = base_client # discover_osds_check_claimed

    method with_osd :
             'a. osd_id : Albamgr_protocol.Protocol.Osd.id ->
                          (Osd.osd -> 'a Lwt.t) -> 'a Lwt.t =
      base_client # with_osd
    method with_nsm_client' :
      'a. namespace_id : int32 ->
                         (Nsm_client.client -> 'a Lwt.t) -> 'a Lwt.t =
      base_client # with_nsm_client'

    method upload_object_from_bytes = base_client # upload_object_from_bytes

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
         self # nsm_host_access # get_nsm_by_id ~namespace_id
         >>= fun nsm_client ->
         Lwt.finalize
           (fun () ->
            let open Nsm_model in
            nsm_client # delete_object
                       ~object_name
                       ~allow_overwrite:(if may_not_exist
                                         then Unconditionally
                                         else AnyPrevious)
            >>= function
            | None ->
               Lwt_log.debug_f
                 "no object with name %s could be found\n"
                 object_name
            | Some old_manifest ->
               (* TODO add en-passant deletion of fragments *)
               Lwt_log.debug_f
                 "object with name %s was deleted\n"
                 object_name)
           (fun () ->
              let () =
                Manifest_cache.ManifestCache.remove
                  (base_client # get_manifest_cache) namespace_id object_name
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
                    Manifest_cache.ManifestCache.remove
                      (base_client # get_manifest_cache)
                      namespace_id
                      object_name;
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
           ManifestCache.invalidate (base_client # get_manifest_cache) namespace_id)

    method drop_cache_by_id namespace_id =
      Manifest_cache.ManifestCache.drop (base_client # get_manifest_cache) namespace_id;
      fragment_cache # drop namespace_id

    method drop_cache namespace =
      self # nsm_host_access # with_namespace_id
        ~namespace
        (self # drop_cache_by_id)

    method deliver_nsm_host_messages ~nsm_host_id =
      Alba_client_message_delivery.deliver_nsm_host_messages
        mgr_access nsm_host_access osd_access
        ~nsm_host_id

    method deliver_osd_messages ~osd_id =
      Alba_client_message_delivery.deliver_osd_messages
        mgr_access nsm_host_access osd_access
        ~osd_id

    method delete_namespace ~namespace =
      Alba_client_namespace.delete_namespace
        mgr_access nsm_host_access
        deliver_nsm_host_messages
        (self # drop_cache_by_id)
        ~namespace

    method decommission_osd ~long_id =
      Alba_client_osd.decommission_osd
        mgr_access osd_access
        ~long_id

    method claim_osd ~long_id =
      Alba_client_osd.claim_osd
        mgr_access osd_access
        ~long_id
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
                ?(osd_timeout = 2.)
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
  let base_client = new Alba_base_client.client
                        cache
                        ~mgr_access
                        ~manifest_cache_size
                        ~bad_fragment_callback
                        ~nsm_host_connection_pool_size
                        ~osd_connection_pool_size
                        ~osd_timeout
  in
  let client = new alba_client base_client in
  Lwt.finalize
  (fun () -> f client)
  (fun () -> cache # close ())
