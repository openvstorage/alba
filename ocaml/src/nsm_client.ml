(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Nsm_protocol
open Protocol
open Lwt.Infix

class client (nsm_host_client : Nsm_host_client.basic_client) namespace_id =
  object(self)
    val update_object_version = ref None

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

    method list_objects_by_policy_raw ~first ~finc ~last ~max =
      self # query
        ListObjectsByPolicy
        RangeQueryArgs.({
            first; finc; last;
            reverse = false; max;
          })

    method list_objects_by_policy ~k ~m ~fragment_count ~max_disks_per_node ~max =
      self # list_objects_by_policy_raw
           ~first:((k,m,fragment_count,max_disks_per_node), "") ~finc:true
           ~last:(Some (((* the sorting of the list by policy index is a bit strange,
                          * max_disks_per_node is negated first, resulting in the need for this ... *)
                         (if max_disks_per_node = 1
                          then (k, m, fragment_count + 1, 0)
                          else (k, m, fragment_count, max_disks_per_node - 1)),
                         ""),
                        false))
           ~max

    method list_objects_by_policy' ~k ~m ~max =
      self # list_objects_by_policy_raw
           ~first:((k, m, 0, 0), "") ~finc:true
           ~last:(Some (((k, m+1, 0, 0), ""), false))
           ~max

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
             Osd_keys.AlbaInstance.to_global_key namespace_id (key, 0, String.length key))
            keys)]

    method get_gc_epochs =
      self # query GetGcEpochs ()

    method get_object_manifest_by_name object_name =
      self # query GetObjectManifestByName object_name

    method get_object_manifests_by_name object_names =
      self # query GetObjectManifestsByName object_names

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

    method update_manifest ~object_name ~object_id
                           (updated_object_locations: Nsm_model.FragmentUpdate.t list)
                           ~gc_epoch ~version_id
      =
      let old_call () =
        let updated_object_locations' =
           List.map
             (fun fu ->
               let open Nsm_model.FragmentUpdate in
               if fu.size_change = None && fu.ctr = None
               then (fu.chunk_id,fu.fragment_id,fu.osd_id_o)
               else failwith "this nsm can't handle that kind of update"
             )
             updated_object_locations
         in
         self # update
              UpdateObject
              (object_name, object_id, updated_object_locations',
               gc_epoch, version_id)
      in
      match !update_object_version with
      | Some 3 ->
         self # update
                  UpdateObject3
                  (object_name, object_id, updated_object_locations,
                   gc_epoch, version_id)
      | Some 2 ->
         self # update
                  UpdateObject2
                  (object_name, object_id, updated_object_locations,
                   gc_epoch, version_id)
      | Some 1 -> old_call ()
      | None ->
         begin
           let call = function
             | 3 ->
                self # update
                     UpdateObject3
                     (object_name, object_id, updated_object_locations,
                      gc_epoch, version_id)
             | 2 ->
                self # update
                     UpdateObject2
                     (object_name, object_id, updated_object_locations,
                      gc_epoch, version_id)

             | 1 ->
                old_call ()
             | _ ->
                let open Nsm_model.Err in
                Lwt.fail (Nsm_exn (Unknown_operation, "UpdateObjectX"))
           in
           let rec loop v =
             Lwt.catch
               (fun () ->
                 call v >>= fun () ->
                 update_object_version := Some v;
                 Lwt.return_unit
               )
               (let open Nsm_model.Err in
                function
                | Nsm_exn (Unknown_operation,_) ->
                   loop (v - 1)
                | exn -> Lwt.fail exn
               )
           in
           loop 3
         end
      | _ -> Lwt.fail_with "programmer error"



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
        ~first:0L
        Std.id
        (self # list_active_osds ~last:None ~max:(-1) ~reverse:false)

    method cleanup_osd_keys_to_be_deleted osd_id =
      self # update
        CleanupOsdKeysToBeDeleted
        osd_id

    method apply_sequence asserts updates =
      self # update
           ApplySequence
           (asserts, updates)
  end
