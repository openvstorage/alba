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
open Lwt.Infix

(* deze client bevat eigen connection pools ...
 * kan concurrent gebruikt worden
 * dus mss best dit gedrag uit OsdPool weghalen
 * en naar osd_wrap_key_value_osd duwen.
 *)
class client alba_id (alba_client : Alba_client.alba_client) =
  let prefix = "TODO" in
  let to_namespace_name namespace_id =
    prefix ^ (serialize ~buf_size:4 Llio.int32_be_to namespace_id)
  in
  let get_kvs ~consistent_read namespace =
    object(self :# Osd.key_value_storage)
      method get_option _prio name =
        let object_name = Slice.get_string_unsafe name in
        alba_client # download_object_to_bytes
                    ~namespace
                    ~object_name
                    ~consistent_read
                    ~should_cache:true

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
           alba_client # get_object_manifest
                       ~namespace ~object_name
                       ~consistent_read ~should_cache:true
           >>= fun (_, r) ->
           match r with
           | None -> Lwt.fail_with "object missing in range entries"
           | Some m ->
              alba_client # download_object_to_bytes
                          ~namespace
                          ~object_name
                          ~consistent_read
                          ~should_cache:true
              >>= function
              | None -> Lwt.fail_with "object missing in range entries (2)"
              | Some v ->
                 Lwt.return (name, v, m.Nsm_model.Manifest.checksum))
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

      method apply_sequence =
        failwith "TODO"
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
                       ~preset_name:None (* TODO which preset? *)
                       () >>= fun _ ->
           Lwt.return ()

    method delete_namespace namespace_id _ =
      alba_client # delete_namespace
                  ~namespace:(to_namespace_name namespace_id) >>= fun () ->
      Lwt.return None

    method set_full _ = failwith "grmbl this method doesn't belong here."
    method get_version = Lwt.return Alba_version.summary
    method get_long_id = alba_id
    method get_disk_usage = Lwt.return (failwith "TODO return sth based on asd disk usage")
  end

let make_client abm_cfg_url tls_config =
  Alba_arakoon.config_from_url abm_cfg_url >>= fun cfg ->
  Alba_client.make_client (ref cfg) ~tls_config () >>= fun (alba_client, closer) ->
  alba_client # mgr_access # get_alba_id >>= fun alba_id ->
  let client = new client alba_id alba_client in
  Lwt.return ((client :> Osd.osd), closer)
