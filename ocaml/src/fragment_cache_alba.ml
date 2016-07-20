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
open Lwt.Infix

type alba_fragment_cache_bucket_strategy =
  | OneOnOne of alba_fragment_cache_bucket_strategy_one_on_one [@name "1-to-1"]
[@@deriving yojson, show]

 and alba_fragment_cache_bucket_strategy_one_on_one = {
     preset : string;
     prefix : string;
   } [@@deriving yojson, show]

class alba_cache
        ~albamgr_cfg_ref
        ~bucket_strategy
        ~nested_fragment_cache
        ~manifest_cache_size
        ~albamgr_connection_pool_size
        ~nsm_host_connection_pool_size
        ~osd_connection_pool_size
        ~osd_timeout
        ~tls_config
        ~partial_osd_read
        ~cache_on_read ~cache_on_write
  =
  let client, closer =
    Alba_client2.make_client
      albamgr_cfg_ref
      ~fragment_cache:nested_fragment_cache
      ~manifest_cache_size
      ~albamgr_connection_pool_size
      ~nsm_host_connection_pool_size
      ~osd_connection_pool_size
      ~osd_timeout
      ~tls_config
      ~partial_osd_read
      ~cache_on_read ~cache_on_write
      ~default_osd_priority:Osd.High
      ~bad_fragment_callback:(fun alba_client
                                  ~namespace_id
                                  ~object_id ~object_name
                                  ~chunk_id ~fragment_id
                                  ~location -> ())
      ~use_fadvise:true
      ~populate_osds_info_cache:true
      ()
  in
  let lru_track = ref (fun ~namespace ~object_name -> ()) in
  let () =
    (let rec get_redis_lru_cache_eviction () =
       Lwt.catch
         (fun () ->
          client # mgr_access # get_maintenance_config >>= fun r ->
          Lwt.return (`Success r.Maintenance_config.redis_lru_cache_eviction))
         (fun exn ->
          Lwt.return `Retry) >>= function
       | `Success x -> Lwt.return x
       | `Retry ->
          Lwt_unix.sleep 10. >>= fun () ->
          get_redis_lru_cache_eviction ()
     in
     get_redis_lru_cache_eviction () >>= function
     | None -> Lwt.return ()
     | Some { Maintenance_config.host; port; key; } ->
        let module R = Redis_lwt.Client in
        let open Lwt_buffer in
        let redis_lru_buffer = Lwt_buffer.create_fixed_capacity 1000 in
        let rec push_items client =
          Lwt_buffer.harvest redis_lru_buffer >>= fun items ->
          let items =
            let t = Unix.gettimeofday () in
            List.map
              (fun (namespace_id, name) ->
               t,
               serialize
                 (Llio.tuple3_to
                    Llio.int8_to
                    Llio.int32_to
                    Llio.string_to)
                 (1, namespace_id, name))
              items
          in
          R.zadd client key items >>= fun _ ->
          push_items client
        in
        let () =
          Lwt_extra2.run_forever
            "redis-lru-cache-registration"
            (fun () ->
             R.with_connection
               R.({ host; port; })
               push_items)
            1.
          |> Lwt.ignore_result
        in
        let () =
          lru_track :=
            fun ~namespace ~object_name ->
            (* TODO lru_track should take namespace_id as an argument instead *)
            (client # nsm_host_access # with_namespace_id
                    ~namespace
                    Lwt.return >>= fun namespace_id ->
             Lwt_buffer.add (namespace_id, object_name) redis_lru_buffer)
            |> Lwt.ignore_result
        in
        Lwt.return ()
    )
    |> Lwt.ignore_result
  in
  let lru_track ~namespace ~object_name = !lru_track ~namespace ~object_name in
  let () =
    (* ignored thread to register that the specified prefix,preset
     * pair is used as a cache by another backend
     *)
    Lwt.ignore_result
      (let rec inner () =
         Lwt.catch
           (fun () ->
            client # mgr_access # update_maintenance_config
                   Maintenance_config.Update.(
              { enable_auto_repair' = None;
                auto_repair_timeout_seconds' = None;
                auto_repair_add_disabled_nodes = [];
                auto_repair_remove_disabled_nodes = [];
                enable_rebalance' = None;
                add_cache_eviction_prefix_preset_pairs = [
                    match bucket_strategy with
                    | OneOnOne { prefix;
                                 preset; } -> (prefix, preset); ];
                remove_cache_eviction_prefix_preset_pairs = [];
                redis_lru_cache_eviction' = None;
              }) >>= fun (_ : Maintenance_config.t) ->
            Lwt.return `Done)
           (fun exn ->
            Lwt_log.info_f ~exn "Exception while registering usage of prefix,preset as a cache" >>= fun () ->
            Lwt.return `Retry) >>= function
         | `Done -> Lwt.return ()
         | `Retry -> Lwt_extra2.sleep_approx 60. >>= fun () ->
                     inner ()
       in
       inner ())
  in
  let base_client = client # get_base_client in
  let make_object_name ~bid ~name =
    match bucket_strategy with
    | OneOnOne _ ->
       name
  in
  let with_namespace bid f =
    match bucket_strategy with
    | OneOnOne { prefix;
                 preset; } ->
       let namespace = Printf.sprintf "%s_%09li" prefix bid in
       Lwt.catch
         (fun () -> f namespace)
         (fun exn ->
          client # create_namespace ~namespace ~preset_name:(Some preset) () >>= fun _ ->
          f namespace)
  in
  object(self)
    inherit Fragment_cache.cache
    method add bid name blob =
      Lwt.catch
        (fun () ->
         with_namespace
           bid
           (fun namespace ->
            let object_name = make_object_name ~bid ~name in
            client # upload_object_from_bigstring_slice
                   ~namespace
                   ~object_name
                   ~object_data:blob
                   ~checksum_o:None
                   ~allow_overwrite:Nsm_model.Unconditionally >>= fun _ ->
            lru_track ~namespace ~object_name;
            Lwt.return_unit))
        (fun exn ->
         Lwt_log.debug_f ~exn "Exception while adding object to alba fragment cache")

    method lookup bid name =
      Lwt.catch
        (fun () ->
         with_namespace
           bid
           (fun namespace ->
            let object_name = make_object_name ~bid ~name in
            client # download_object_to_bytes
                   ~object_name
                   ~namespace
                   ~consistent_read:false
                   ~should_cache:true
            |> Lwt.map
                 (Option.map
                    (fun x ->
                     lru_track ~namespace ~object_name;
                     fst x)
                 )))
        (fun exn ->
         Lwt_log.debug_f ~exn "Exception during alba fragment cache lookup" >>= fun () ->
         Lwt.return_none)

    method lookup2 bid name object_slices =
      let object_slices =
        List.map
          (fun (offset, length, dest, dest_off) ->
           Int64.of_int offset, length, dest, dest_off)
          object_slices
      in
      Lwt.catch
        (fun () ->
         with_namespace
           bid
           (fun namespace ->
            let object_name = make_object_name ~bid ~name in
            client # nsm_host_access # with_namespace_id
                   ~namespace
                   (fun namespace_id ->
                    base_client # download_object_slices'
                                ~namespace_id
                                ~object_name
                                ~object_slices
                                ~consistent_read:false
                                ~fragment_statistics_cb:ignore
                   ) >>= function
            | None -> Lwt.return_false
            | Some _ ->
               lru_track ~namespace ~object_name;
               Lwt.return_true))
        (fun exn ->
         Lwt_log.debug_f ~exn "Exception during alba fragment cache lookup2" >>= fun () ->
         Lwt.return_false)

    method drop bid ~global =
      if global
      then
        Lwt_extra2.ignore_errors
          ~logging:true
          (fun () ->
           with_namespace
             bid
             (fun namespace -> client # delete_namespace ~namespace))
      else
        Lwt.return_unit

    method close () = closer ()
  end
