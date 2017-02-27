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
open Slice
open Alba_based_osd
open Lwt.Infix

(* TODO what about statistics? *)

class client
        (alba_client : Alba_client.alba_client)
        ~alba_id ~prefix ~preset_name
        ~namespace_name_format
  =
  let to_namespace_name = to_namespace_name prefix namespace_name_format in
  let get_kvs ~consistent_read namespace =
    object(self :# Osd.key_value_storage)
      method get_option _prio name =
        let object_name = Slice.get_string_unsafe name in
        alba_client # download_object_to_bytes
                    ~namespace
                    ~object_name
                    ~consistent_read
                    ~should_cache:true
        |> Lwt.map (Option.map (fun (d, _, _) -> d))

      method get_exn prio name =
        self # get_option prio name >>= function
        | None -> Lwt.fail_with
                    (Printf.sprintf
                       "Could not get value for key %S"
                       (Slice.get_string_unsafe name))
        | Some v -> Lwt.return v

      method multi_get prio names =
        (* TODO could (in batch) ensure all manifest are available *)
        Lwt_list.map_p
          (self # get_option prio)
          names

      method multi_exists _prio names =
        alba_client # nsm_host_access
                    # with_nsm_client
                    ~namespace
                    (fun nsm -> nsm # multi_exists
                                    (List.map
                                       Slice.get_string_unsafe
                                       names))

      method range _prio ~first ~finc ~last ~reverse ~max =
        alba_client # list_objects
                    ~namespace
                    ~first:(Slice.get_string_unsafe first) ~finc
                    ~last:(Option.map
                             (fun (l, linc) ->
                              Slice.get_string_unsafe l, linc)
                             last)
                    ~reverse ~max
        >>= fun ((cnt, names), has_more) ->
        Lwt.return ((cnt, List.map Slice.wrap_string names), has_more)

      method range_entries prio ~first ~finc ~last ~reverse ~max =
        self # range
          prio
          ~first ~finc ~last
          ~reverse ~max
        >>= fun ((cnt, names), has_more) ->
        Lwt_list.map_p
          (fun name ->
           let object_name = Slice.get_string_unsafe name in
           alba_client # download_object_to_bytes
                       ~namespace
                       ~object_name
                       ~consistent_read
                       ~should_cache:true
           >>= function
           | None -> Lwt.fail_with "object missing in range entries"
           | Some (v, mf, _) ->
              Lwt.return (name, v, mf.Nsm_model.Manifest.checksum))
          names >>= fun r ->
        Lwt.return ((cnt, r), has_more)

      method partial_get prio name object_slices =
        alba_client # download_object_slices
                    ~namespace
                    ~object_name:(Slice.get_string_unsafe name)
                    ~object_slices:(List.map
                                      (fun (offset, len, dst, dstoff) ->
                                       Int64.of_int offset, len, dst, dstoff)
                                      object_slices)
                    ~consistent_read
                    ~fragment_statistics_cb:ignore >>= function
        | None -> Lwt.return Osd.NotFound
        | Some _ -> Lwt.return Osd.Success

      method apply_sequence prio asserts updates =

        let apply_sequence_updates =
          List.map
            (function
             | Osd.Update.Set (key, None) ->
                Alba_client_sequence.DeleteObject (Slice.get_string_unsafe key)
             | Osd.Update.Set (key, Some (value, cs, _)) ->
                let object_reader =
                  let open Asd_protocol.Blob in
                  match value with
                  | Lwt_bytes s -> new Object_reader.bytes_reader s
                  | Bigslice s -> new Object_reader.bigstring_slice_reader s
                  | Bytes s -> new Object_reader.string_reader s
                  | Slice s -> new Object_reader.slice_reader s
                in
                Alba_client_sequence.UploadObjectFromReader
                  (Slice.get_string_unsafe key,
                   object_reader,
                   Some cs))
            updates
        in

        let assert_ts namespace_id =
          Lwt_list.map_p
            (function
              | Osd.Assert.Value (key, None) ->
                 Lwt.return (Nsm_model.Assert.ObjectDoesNotExist (Slice.get_string_unsafe key))
              | Osd.Assert.Value (key, Some blob) ->
                 (* TODO can be optimized to not download the object
                  * if the object was stored with a checksum *)
                 let object_name = Slice.get_string_unsafe key in
                 alba_client # download_object_to_bytes
                             ~namespace
                             ~object_name
                             ~consistent_read:true
                             ~should_cache:true >>= function
                 | None -> Lwt.fail Nsm_model.Err.(Nsm_exn (Assert_failed, object_name))
                 | Some (v, mf, namespace_id') ->
                    assert (namespace_id = namespace_id');
                    if Osd.Blob.equal blob (Osd.Blob.Lwt_bytes v)
                    then Lwt.return (Nsm_model.Assert.ObjectHasId
                                       (object_name,
                                        mf.Nsm_model.Manifest.object_id))
                    else Lwt.fail Nsm_model.Err.(Nsm_exn (Assert_failed, object_name)))
            asserts
        in

        alba_client
          # nsm_host_access
          # with_namespace_id
          ~namespace
          (fun namespace_id ->
            Lwt.catch
              (fun () ->
                Alba_client_sequence.apply_sequence
                  alba_client
                  namespace_id
                  (assert_ts namespace_id)
                  apply_sequence_updates >>= fun _ ->
                Lwt.return (Osd.Ok []))
              (function
               | Nsm_model.Err.Nsm_exn (Nsm_model.Err.Assert_failed, key) ->
                  Lwt.return (Osd.Exn Osd.Error.(Assert_failed key))
               | exn ->
                  Lwt.fail exn))
    end
  in
  object(self :# Osd.osd)

    method global_kvs =
      get_kvs ~consistent_read:true prefix

    method namespace_kvs namespace_id =
      get_kvs ~consistent_read:false (to_namespace_name namespace_id)

    method add_namespace namespace_id =
      alba_client # mgr_access # get_namespace
                  ~namespace:(to_namespace_name namespace_id) >>= fun r ->
      if (r <> None)
      then Lwt.return ()
      else alba_client # create_namespace
                       ~namespace:(to_namespace_name namespace_id)
                       ~preset_name
                       () >>= fun _ ->
           Lwt.return ()

    method delete_namespace namespace_id _ =
      let namespace = to_namespace_name namespace_id in
      alba_client # mgr_access # get_namespace ~namespace >>= function
      | None -> Lwt.return_none
      | Some _ ->
         alba_client # delete_namespace
                     ~namespace >>= fun () ->
         Lwt.return_none

    method set_full _ = failwith "grmbl this method doesn't belong here."
    method set_slowness _ = failwith "set_slowness not implemented"

    method get_version = Lwt.return Alba_version.summary
    method get_long_id = alba_id
    method get_disk_usage = Lwt.return
                              (1000L, 2000L)
                              (* (failwith "TODO return sth based on asd disk usage") *)
    method capabilities = Lwt.return (0, [])
  end

let rec make_client
          abm_cfg
          ~tls_config
          ~tcp_keepalive
          ~prefix
          ~preset_name
          ~albamgr_connection_pool_size
          ~namespace_name_format
          ~upload_slack
          ~osd_connection_pool_size
          ~osd_timeout
          ~default_osd_priority
  =
  let albamgr_pool =
    Remotes.Pool.Albamgr.make
      ~size:albamgr_connection_pool_size
      abm_cfg
      tls_config
      Buffer_pool.default_buffer_pool
      ~tcp_keepalive
  in
  let mgr_access = Albamgr_access.make albamgr_pool in
  let osd_access =
    new Osd_access.osd_access mgr_access
        ~osd_connection_pool_size
        ~osd_timeout
        ~default_osd_priority
        ~tls_config
        ~tcp_keepalive
        (make_client
           ~albamgr_connection_pool_size
           ~upload_slack
           ~osd_connection_pool_size
           ~osd_timeout
           ~default_osd_priority
        )
  in
  let alba_client, closer =
    Alba_client.make_client
      mgr_access
      ~osd_access
      ~tls_config
      ~populate_osds_info_cache:true
      ~upload_slack
      ()
  in
  let closer () =
    mgr_access # finalize;
    osd_access # finalize;
    closer ()
  in
  Lwt.catch
    (fun () ->
     alba_client # mgr_access # get_alba_id >>= fun alba_id ->
     begin
       (* ensure the global prefix namespace exists *)
       let namespace = prefix in
       alba_client # mgr_access # get_namespace ~namespace >>= function
       | Some _ -> Lwt.return ()
       | None ->
          alba_client # create_namespace ~namespace ~preset_name () >>= fun _ ->
          Lwt.return ()
     end >>= fun () ->
     let client =
       new client
           alba_client
           ~alba_id
           ~prefix ~preset_name
           ~namespace_name_format
     in
     Lwt.return ((client :> Osd.osd), closer))
    (fun exn ->
     closer () >>= fun () ->
     Lwt.fail exn)
