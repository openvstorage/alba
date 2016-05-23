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
open Nsm_protocol
open Protocol
open Lwt.Infix

class client (nsm_host_client : Nsm_host_client.basic_client) namespace_id =
  object(self)

    method private query : type req res.
      ?consistency : Consistency.t ->
      (req, res) query -> req -> res Lwt.t =
      fun ?consistency command req ->
        nsm_host_client # query
          ?consistency
          (Nsm_host_protocol.Protocol.NsmQuery command)
          (namespace_id, req)

    method private update : type req res. (req, res) update -> req -> res Lwt.t =
      fun command req ->
        nsm_host_client # update
          (Nsm_host_protocol.Protocol.NsmUpdate command)
          (namespace_id, req)

    method list_objects ~first ~finc ~last ~max ~reverse =
      self # query
        ListObjects
        RangeQueryArgs.({ first; finc;
                          last; max;
                          reverse; })

    method list_all_objects () =
      list_all_x
        ~first:""
        Std.id
        (self # list_objects ~last:None ~max:(-1) ~reverse:false)

    method list_objects_by_id ~first ~finc ~last ~max ~reverse =
      self # query
        ListObjectsById
        RangeQueryArgs.({ first; finc;
                          last; max;
                          reverse; })

    method list_device_objects ~osd_id ~first ~finc ~last ~max ~reverse =
      self # query
        ListObjectsByOsd
        (osd_id,
         RangeQueryArgs.{
           first; finc; last;
           max; reverse; })

    method list_objects_by_policy ~k ~m ~max =
      self # query
        ListObjectsByPolicy
        RangeQueryArgs.({
            first = ((k, m, 0, 0), ""); finc = true;
            last = Some (((k, m+1, 0, 0), ""), false);
            reverse = false;
            max;
          })

    method list_objects_by_policy' ~first ~finc ~last ~max =
      self # query
        ListObjectsByPolicy
        RangeQueryArgs.({
            first; finc; last;
            reverse = false; max;
          })

    method multi_exists names =
      self # query
           MultiExists
           names

    method list_device_keys_to_be_deleted ~osd_id ~first ~finc ~last ~max ~reverse =
      self # query
        ListDeviceKeysToBeDeleted
        (osd_id,
         RangeQueryArgs.(
           {
             first; finc; last;
             max; reverse;
           })) >>= fun ((cnt, keys), has_more) ->
      Lwt.return ((cnt,
                   List.map
                     (fun key ->
                      let cnt = Osd_keys.AlbaInstance.verify_global_key namespace_id (key, 0) in
                      Str.string_after key cnt)
                     keys),
                  has_more)

    method mark_keys_deleted ~osd_id ~keys =
      self # update
        MarkKeysDeleted
        [(osd_id,
          List.map
            (fun key ->
             Osd_keys.AlbaInstance.to_global_key namespace_id (key, 0, 0))
            keys)]

    method get_gc_epochs =
      self # query GetGcEpochs ()

    method get_object_manifest_by_name object_name =
      self # query GetObjectManifestByName object_name

    method get_object_manifest_by_id object_id =
      self # query GetObjectManifestById object_id

    method get_stats =
      self # query GetStats ()


    method put_object ~allow_overwrite ~gc_epoch ~manifest =
      self # update
        PutObject
        (allow_overwrite, manifest, gc_epoch)

    method delete_object ~object_name ~allow_overwrite =
      self # update
        DeleteObject
        (allow_overwrite, object_name)

    method update_manifest ~object_name ~object_id updated_object_locations ~gc_epoch ~version_id =
      self # update
        UpdateObject
        (object_name, object_id, updated_object_locations, gc_epoch, version_id)

    method enable_gc_epoch gc_epoch =
      self # update
        EnableGcEpoch
        gc_epoch

    method disable_gc_epoch gc_epoch =
      self # update
        DisableGcEpoch
        gc_epoch

    method list_active_osds ~first ~finc ~last ~max ~reverse =
      self # query
        ListActiveOsds
        RangeQueryArgs.({
            first; finc; last;
            max; reverse;
          })

    method list_all_active_osds =
      list_all_x
        ~first:0l
        Std.id
        (self # list_active_osds ~last:None ~max:(-1) ~reverse:false)

    method cleanup_osd_keys_to_be_deleted osd_id =
      self # update
        CleanupOsdKeysToBeDeleted
        osd_id
  end
