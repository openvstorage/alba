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
open Fragment_cache
open Alba_statistics
open Alba_client_errors
open Lwt.Infix
open Lwt_bytes2

class alba_client (base_client : Alba_base_client.client)
  =
  let mgr_access = base_client # mgr_access in
  let nsm_host_access = base_client # nsm_host_access in
  let osd_access = base_client # osd_access in

  let alba_id_t =
    let rec inner () =
      Lwt.catch
        (fun () ->
          mgr_access # get_alba_id >>= fun x -> Lwt.return (Some x))
        (fun exn ->
          Lwt_log.info_f ~exn "Exception in alba_id_t" >>= fun () ->
          Lwt.return_none) >>= function
      | Some x -> Lwt.return x
      | None ->
         Lwt_unix.sleep 1. >>= fun () ->
         inner ()
    in
    inner ()
  in

  object(self)
    method get_alba_id = alba_id_t

    method get_base_client = base_client
    method get_manifest_cache = base_client # get_manifest_cache
    method mgr_access = mgr_access
    method nsm_host_access = nsm_host_access
    method osd_access = osd_access

    method get_object_manifest' = base_client # get_object_manifest'
    method download_object_slices = base_client # download_object_slices

    method create_namespace ~namespace ~preset_name ?nsm_host_id () =
      Alba_client_namespace.create_namespace
        mgr_access
        (base_client # get_preset_info)
        (base_client # deliver_messages_to_most_osds)
        (base_client # deliver_nsm_host_messages)
        ~namespace ~preset_name ?nsm_host_id ()

    method upload_object_from_file = base_client # upload_object_from_file

    method osd_infos =
      alba_id_t >>= fun alba_id ->
      let my_entry = (alba_id, osd_access # osd_infos) in
      base_client # get_fragment_cache # osd_infos () >>= fun (cnt, osd_infos) ->
      Lwt.return (cnt + 1, my_entry::osd_infos)

    method discover_osds = base_client # discover_osds

    method with_osd :
             'a. osd_id : Albamgr_protocol.Protocol.Osd.id ->
                          (Osd.osd -> 'a Lwt.t) -> 'a Lwt.t =
      base_client # with_osd
    method with_nsm_client' :
      'a. namespace_id : int32 ->
                         (Nsm_client.client -> 'a Lwt.t) -> 'a Lwt.t =
      base_client # with_nsm_client'

    method upload_object_from_bytes = base_client # upload_object_from_bytes
    method upload_object_from_bigstring_slice = base_client # upload_object_from_bigstring_slice

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
      let dest = Lwt_bytes.create length in
      Lwt.finalize
        (fun () ->
         self # download_object_slices
              ~namespace
              ~object_name
              ~object_slices:(let acc, _ =
                                List.fold_left
                                  (fun (acc, dest_off) (slice_offset, slice_length) ->
                                   (slice_offset, slice_length, dest, dest_off) :: acc,
                                   dest_off + slice_length)
                                  ([], 0)
                                  object_slices
                              in
                              List.rev acc)
              ~consistent_read
              ~fragment_statistics_cb:ignore >>= function
         | None -> Lwt.return None
         | Some _ -> Lwt.return (Some (Lwt_bytes.to_string dest)))
        (fun () ->
         Lwt_bytes2.Lwt_bytes.unsafe_destroy dest;
         Lwt.return_unit)

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
         : (Nsm_model.Manifest.t
            * Statistics.object_download
            * int32) option Lwt.t

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
      (match !fd_ref with
       | None ->
          Lwt_unix.openfile
            output_file
            Lwt_unix.([O_WRONLY; O_CREAT; O_TRUNC])
            0o664 >>= fun fd ->
          fd_ref := Some fd;
          Lwt.return fd
       | Some fd ->
          Lwt_unix.ftruncate fd 0 >>= fun () ->
          Lwt_unix.lseek fd 0 Lwt_unix.SEEK_SET >>= fun _ ->
          Lwt.return fd) >>= fun fd ->
      (* TODO could fallocate here *)
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
    >>= function
    | None -> Lwt.return_none
    | Some _ -> Lwt.return !res

  method download_object_to_bytes
           ~namespace
           ~object_name
           ~consistent_read
           ~should_cache
         : (Lwt_bytes.t * Nsm_model.Manifest.t * int32) option Lwt.t =
    let res = ref None in
    let write_object_data total_size =
      let bs = Lwt_bytes.create (Int64.to_int total_size) in
      res := Some bs;
      let offset = ref 0 in
      let write source pos len =
        Lwt_bytes.blit source pos bs !offset len;
        offset := !offset + len;
        Lwt.return ()
      in
      Lwt.return write
    in
    let destroy_res () =
      match !res with
      | None -> ()
      | Some x -> Lwt_bytes.unsafe_destroy x
    in
    Lwt.catch
      (fun () ->
       self # download_object_generic
            ~namespace
            ~object_name
            ~write_object_data
            ~consistent_read
            ~should_cache
       >>= function
       | Some (mf, _, namespace_id) ->
          Lwt.return (Some (Option.get_some !res, mf, namespace_id))
       | None ->
          let () = destroy_res () in
          Lwt.return_none)
      (fun exn ->
       let () = destroy_res () in
       Lwt.fail exn)

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
    >>= function
    | None -> Lwt.return_none
    | Some _ -> Lwt.return !res

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
         : (Nsm_model.Manifest.t
            * Statistics.object_download
            * int32) option Lwt.t

      =
      let t0_object = Unix.gettimeofday () in
      with_timing_lwt
        (fun () -> self # get_object_manifest'
                        ~namespace_id ~object_name
                        ~consistent_read ~should_cache)
      >>= fun r ->
      match r with
      | _, (hit_or_miss, None) -> Lwt.return None
      | (t_get_manifest, (mf_src, Some manifest)) ->
        let open Nsm_model in
        Lwt_log.debug_f
          "download_object: found %S, mf_src=%s manifest = %s"
          object_name
          (Cache.show_value_source mf_src)
          (Manifest.show manifest) >>= fun () ->

        let get_manifest_dh = t_get_manifest, mf_src in
        let attempt_download get_manifest_dh manifest =
          write_object_data manifest.Manifest.size >>= fun write_object_data ->
          base_client # download_object_generic''
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
                 with_timing_lwt
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
                      let get_manifest_dh' = (delay, Cache.Stale) in
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

    method deliver_nsm_host_messages =
      base_client # deliver_nsm_host_messages

    method deliver_osd_messages ~osd_id =
      Alba_client_message_delivery.deliver_osd_messages
        mgr_access nsm_host_access osd_access
        ~osd_id

    method drop_cache = base_client # drop_cache

    method delete_namespace = base_client # delete_namespace

    method decommission_osd ~long_id =
      Alba_client_osd.decommission_osd
        mgr_access osd_access
        ~long_id

    method claim_osd ~long_id =
      Alba_client_osd.claim_osd
        mgr_access osd_access
        ~long_id
  end

let make_client (mgr_access : Albamgr_access.mgr_access)
                ~osd_access
                ?(fragment_cache = (new no_cache :> cache))
                ?(manifest_cache_size=100_000)
                ?(bad_fragment_callback = fun
                    alba_client
                    ~namespace_id ~object_id ~object_name
                    ~chunk_id ~fragment_id ~location -> ())
                ?(nsm_host_connection_pool_size = 10)
                ~tls_config
                ?(release_resources = false)
                ?(tcp_keepalive = Tcp_keepalive2.default)
                ?(use_fadvise = true)
                ?(partial_osd_read = true)
                ?(cache_on_read = true)
                ?(cache_on_write = true)
                ~populate_osds_info_cache
                ()
  =
  let base_client = new Alba_base_client.client
                        fragment_cache
                        ~mgr_access:(mgr_access :> Albamgr_client.client)
                        ~osd_access
                        ~manifest_cache_size
                        ~bad_fragment_callback
                        ~nsm_host_connection_pool_size
                        ~tls_config
                        ~use_fadvise
                        ~partial_osd_read
                        ~cache_on_read ~cache_on_write
                        ~populate_osds_info_cache
  in
  let client = new alba_client base_client in
  let closer () =
    if release_resources
    then
      begin
        base_client # osd_access # finalize;
        base_client # nsm_host_access # finalize;
        mgr_access # finalize;
        fragment_cache # close ()
      end
    else
      Lwt.return ()
  in
  (client, closer)

let with_client albamgr_client_cfg
                ~osd_access
                ?fragment_cache
                ?manifest_cache_size
                ?bad_fragment_callback
                ?nsm_host_connection_pool_size
                ~tls_config
                ?release_resources
                ?tcp_keepalive
                ?use_fadvise
                ?partial_osd_read
                ?cache_on_read
                ?cache_on_write
                ~populate_osds_info_cache
                f
  =
  let client, closer =
    make_client albamgr_client_cfg
              ~osd_access
              ?fragment_cache
              ?manifest_cache_size
              ?bad_fragment_callback
              ?nsm_host_connection_pool_size
              ~tls_config
              ?release_resources
              ?tcp_keepalive
              ?use_fadvise
              ?partial_osd_read
              ?cache_on_read
              ?cache_on_write
              ~populate_osds_info_cache
              ()
  in
  Lwt.finalize
    (fun () -> f client)
    closer
