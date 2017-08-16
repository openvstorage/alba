(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
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
        ~albamgr_refresh_config
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
      ~albamgr_refresh_config
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
      ~upload_slack:1.0
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
        let redis_lru_buffer = Lwt_buffer.create ~capacity:(Some 1000) ~leaky:true () in
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
                    x_int64_to
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
  let get_namespace_name bid =
    match bucket_strategy with
    | OneOnOne { prefix;
                 preset; } ->
       Printf.sprintf "%s_%09Li" prefix bid
  in
  let with_namespace bid f =
    match bucket_strategy with
    | OneOnOne { prefix;
                 preset; } ->
       let namespace = get_namespace_name bid in
       Lwt.catch
         (fun () -> f namespace)
         (fun exn ->
           Lwt.catch
             (client # create_namespace ~namespace ~preset_name:(Some preset))
             (let open Albamgr_protocol.Protocol.Error in
              function
              | Albamgr_exn (Namespace_already_exists, _) -> Lwt.return 0L
              | exn -> Lwt.fail exn)
           >>= fun _ ->
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
                   ~epilogue_delay:None
                   ~namespace
                   ~object_name
                   ~object_data:blob
                   ~checksum_o:None
                   ~allow_overwrite:Nsm_model.Unconditionally
            >>= fun (mf, mfs, _, namespace_id) ->
            lru_track ~namespace ~object_name;
            client # get_alba_id >>= fun alba_id ->
            Lwt.return ((mf, namespace_id, alba_id) :: mfs)
           )
        )
        (fun exn ->
          Lwt_log.info_f ~exn "Exception while adding object to alba fragment cache"
          >>= fun () ->
          Lwt.return []
        )

    method lookup ~timeout bid name =
      Lwt_extra2.with_timeout_default
        ~msg:"fragment_cache_alba # lookup"
        timeout
        None
        (fun () ->
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
                  >>= function
                  | None -> Lwt.return_none
                  | Some (data, mf, namespace_id) ->
                     let open Lwt_bytes2 in
                     let sb = SharedBuffer.make_shared data in
                     lru_track ~namespace ~object_name;
                     client # get_alba_id >>= fun alba_id ->
                     Lwt.return (Some (sb, [ mf, namespace_id, alba_id ])))
            )
            (fun exn ->
              Lwt_log.debug_f ~exn "Exception during alba fragment cache lookup"
              >>= fun () ->
              Lwt.return_none)
        )

    method lookup2 ~timeout bid name object_slices =
      Lwt_extra2.with_timeout_default
        ~msg:"fragment_cache_alba # lookup2"
        timeout
        (false,[])
        (fun () ->
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
                  | None -> Lwt.return (false, [])
                  | Some (mf, namespace_id, _, mfs) ->
                     lru_track ~namespace ~object_name;
                     client # get_alba_id >>= fun alba_id ->
                     let mfs = (mf, namespace_id, alba_id) :: mfs in
                     Lwt.return (true, mfs)))
            (fun exn ->
              Lwt_log.debug_f ~exn "Exception during alba fragment cache lookup2" >>= fun () ->
              Lwt.return (false, []))
        )

    method drop bid ~global =
      if global
      then
        let namespace = get_namespace_name bid  in
        let rec cleanup_namespace () =
          Lwt.catch
            (fun () ->
              client # mgr_access # get_namespace ~namespace >>= function
              | None -> Lwt.return `Done
              | Some _ ->
                 client # delete_namespace ~namespace >>= fun () ->
                 Lwt.return `Done)
            (fun exn ->
              Lwt_log.info_f ~exn
                             "Cleanup of fragment cache namespace %s failed, will retry in a minute"
                             namespace >>= fun () ->
              Lwt.return `Retry)
          >>= function
          | `Done ->
             Lwt.return ()
          | `Retry ->
             Lwt_extra2.sleep_approx 60. >>= fun () ->
             cleanup_namespace ()
        in

        cleanup_namespace () >>= fun () ->

        (* delete the namespace again after some delay, as otherwise
         * there might be namespace left due to a race between namespace
         * deletion and possible uploads/downloads that were still ongoing.
         *
         * (eventually the namespace would get deleted when the fragment cache
         *  gets full, however that might take a long time, and in some scenarios
         *  this could lead to many useless namespaces, which then again leads
         *  to plenty of nsm_host arakoon clusters.)
         *)
        Lwt_extra2.sleep_approx 120. >>= fun () ->
        cleanup_namespace ()
      else
        Lwt.return_unit

    method close () = closer ()

    method osd_infos () = client # osd_infos
    method has_local_fragment_cache = client # has_local_fragment_cache
    method get_preset ~alba_id ~namespace_id = client # get_preset ~alba_id ~namespace_id
  end
