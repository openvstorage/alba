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

open! Prelude
open Lwt.Infix

let do_random_eviction
      (alba_client : Alba_base_client.client)
      ~prefixes
      percentage_to_clean_up
  =
  Lwt_log.info_f "Alba_eviction: starting random eviction" >>= fun () ->
  Lwt_list.map_p
    (fun prefix ->
     alba_client # mgr_access # list_all_namespaces_with_prefix prefix)
    prefixes >>= fun namespaces ->
  let namespaces =
    List.fold_left
      (fun acc (_, namespaces) ->
       List.rev_append namespaces acc)
      []
      namespaces
  in
  Lwt_list.map_p
    (fun (name, namespace) ->
     alba_client # with_nsm_client
                 ~namespace:name
                 (fun nsm_client ->
                  nsm_client # get_stats >>= fun stats ->
                  Lwt.return (name, namespace.Albamgr_protocol.Protocol.Namespace.id, stats)
                 )
    )
    namespaces
  >>= fun r ->
  let (namespaces_with_count, empty_namespaces) =
    List.fold_left
      (fun (namespaces_with_count, empty_namespaces)
           (namespace_name, namespace_id, stats) ->
       let obj_cnt =
         List.fold_left
           (fun acc (_, cnt) ->
            Int64.add acc cnt)
           0L
           (snd stats.Nsm_model.NamespaceStats.bucket_count)
       in
       let has_objects = obj_cnt > 0L in
       if has_objects
       then ((namespace_id, obj_cnt) :: namespaces_with_count), empty_namespaces
       else namespaces_with_count, (namespace_name :: empty_namespaces)
      )
      ([], [])
      r
  in
  (* remove a percentage of objects from each namespace *)
  let delete_objects () =
    Lwt_list.iter_p
      (fun (namespace_id, obj_cnt) ->
        alba_client
          # with_nsm_client'
          ~namespace_id
          (fun nsm_client ->
            let first, last, reverse = make_first_last_reverse () in
            let rec inner = function
              | 0 -> Lwt.return ()
              | todo ->
                 nsm_client # list_objects_by_id
                            ~first ~finc:true
                            ~last ~reverse
                            ~max:todo >>= fun ((cnt, objs), has_more) ->
                 Lwt_list.iter_s
                   (fun manifest ->
                     nsm_client # delete_object
                                ~object_name:manifest.Nsm_model.Manifest.name
                                ~allow_overwrite:Nsm_model.AnyPrevious
                     >|= ignore)
                   objs >>= fun () ->
                 if has_more
                 then inner (todo - cnt)
                 else Lwt.return ()
            in
            let victims =
              let open Int64 in
              let cnt' = mul obj_cnt percentage_to_clean_up in
              let remainder = rem cnt' 100L in
              let result = div cnt' 100L in
              (* round number of victims up, so we can completely clean out almost empty namespaces *)
              add result (if remainder > 0L then 1L else 0L) |> to_int
            in
            inner victims
          )
      )
      namespaces_with_count
  in
  let delete_empty_namespaces () =
    Lwt_list.iter_p
      (fun namespace ->
        Lwt_log.info_f "Alba_eviction: deleting namespace %s because it's empty" namespace >>= fun () ->
        alba_client # delete_namespace ~namespace)
      empty_namespaces
  in
  Lwt.join
    [ delete_objects ();
      delete_empty_namespaces ();
    ]

let should_evict (alba_client : Alba_base_client.client) coordinator =
  if coordinator # is_master
  then
    begin
      let cnt_total, cnt_full, fill_ratio, _ =
        Hashtbl.fold
          (fun osd_id (osd_info, osd_state, _)
               (cnt_total, cnt_full, avg_fill_ratio, acc) ->
           let open Nsm_model in
           let used = Int64.to_float osd_info.OsdInfo.used in
           let total = Int64.to_float osd_info.OsdInfo.total in
           let fill_ratio = used /. total in
           let full = fill_ratio > 0.85 in
           (cnt_total + 1,
            cnt_full + (if full then 1 else 0),
            (if full
             then (avg_fill_ratio *. (float cnt_full) +. fill_ratio) /. ((cnt_full + 1) |> float)
             else avg_fill_ratio),
            (osd_id,
             used, total,
             fill_ratio)
            :: acc))
          (alba_client # osd_access # osds_info_cache)
          (0, 0, 0., [])
      in
      (* more than half of the disks are >90% filled,
       * so let's do some cleanup *)
      if (float cnt_total /. float cnt_full) < 2.
      then `True fill_ratio
      else `False
    end
  else
    `False

let lru_collect_some_garbage alba_client redis_client key =
  Lwt_log.info_f "Alba_eviction: starting redis based lru eviction" >>= fun () ->
  let module R = Redis_lwt.Client in
  R.zrangebyscore
    redis_client
    key
    R.FloatBound.NegInfinity R.FloatBound.PosInfinity
    ~limit:(0, 100)
  >>= fun items ->

  let items =
    List.map
      (fun r ->
       match r with
       | `Bulk (Some b) -> b
       | x -> assert false)
      items
  in
  let items' =
    List.map
      (fun b ->
       let version, namespace_id, name =
         deserialize
           (Llio.tuple3_from
              Llio.int8_from
              x_int64_from
              Llio.string_from)
           b
       in
       assert (version = 1);
       namespace_id, name)
      items
    |> List.group_by
         (fun (namespace_id, _) -> namespace_id)
    |> Hashtbl.to_assoc_list
  in

  Lwt_list.map_p
    (fun (namespace_id, names) ->

      Lwt.catch
        (fun () ->
          alba_client # nsm_host_access # get_nsm_by_id
                      ~namespace_id >>= fun client ->

          let names = List.map snd names in
          client # get_object_manifests_by_name names >>= fun mfs ->

          let size =
            List.fold_left
              (fun acc ->
                function
                | None -> acc
                | Some mf ->
                   Int64.(add
                            acc
                            (Nsm_model.Manifest.get_summed_fragment_sizes mf))
              )
              0L
              mfs
          in

          client # apply_sequence
                 []
                 (List.map
                    (fun name -> Nsm_model.Update.DeleteObject name)
                    names) >>= fun () ->
          Lwt.return size
        )
        (fun exn ->
          (match exn with
           | Albamgr_protocol.Protocol.Error.Albamgr_exn(Albamgr_protocol.Protocol.Error.Namespace_does_not_exist, _) ->
              Lwt.return_unit
           | exn ->
              Lwt_log.info_f ~exn "Ignoring exception during Alba_eviction.lru_collect_some_garbage") >>= fun () ->
          Lwt.return 0L)
    )
    items'

  >>= fun sizes ->

  (if List.length items > 0
   then
     R.zrem redis_client key items
   else
     Lwt.return 0)

  >>= fun _ -> Lwt.return (List.fold_left Int64.add 0L sizes)
