(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Lwt.Infix

let write_albamgr_cfg albamgr_cfg =
  let value = Arakoon_client_config.to_ini albamgr_cfg in
  function
  | Url.File destination ->
     let tmp = destination ^ ".tmp" in
     Lwt_extra2.unlink ~fsync_parent_dir:false  ~may_not_exist:true tmp >>= fun () ->
     Lwt_extra2.with_fd
       tmp
       ~flags:Lwt_unix.([ O_WRONLY; O_CREAT; O_EXCL; ])
       ~perm:0o664
       (fun fd ->
        Lwt_extra2.write_all
          fd
          value 0 (String.length value) >>= fun () ->
        Lwt_unix.fsync fd) >>= fun () ->
     Lwt_extra2.rename ~fsync_parent_dir:true tmp destination
  | Url.Etcd (peers, path) ->
     Arakoon_etcd.store_value peers path value
  | Url.Arakoon { Url.cluster_id; key; ini_location; } ->
     Arakoon_config_url.(retrieve (File ini_location)) >|= Arakoon_client_config.from_ini
     >>= fun ccfg ->
     Client_helper.with_master_client'
       ccfg
       (fun client -> client # set key value)


let refresh_albamgr_cfg
    ~loop
    albamgr_client_cfg
    (alba_client : Alba_client.alba_client)
    ~tls_config
    ~tcp_keepalive
    destination =

  let rec inner () =
    Lwt_log.debug "refresh_albamgr_cfg" >>= fun () ->
    let open Albamgr_client in
    Lwt.catch
      (fun () ->
         alba_client # mgr_access # get_client_config
         >>= fun ccfg ->
         Lwt.return (Res ccfg))
      (let open Client_helper.MasterLookupResult in
       function
       | Arakoon_exc.Exception(Arakoon_exc.E_NOT_MASTER, master)
       | Error (Unknown_node (master, (_, _))) ->
          retrieve_cfg_from_any_node ~tls_config !albamgr_client_cfg
       | exn ->
          Lwt_log.debug_f ~exn "refresh_albamgr_cfg failed" >>= fun () ->
          Lwt.return Retry
      )
    >>= function
    | Retry ->
      Lwt_extra2.sleep_approx 60. >>= fun () ->
      inner ()
    | Res ccfg ->
      albamgr_client_cfg := ccfg;
      Lwt.catch
        (fun () -> write_albamgr_cfg ccfg destination)
        (fun exn ->
         Lwt_log.info_f
           ~exn
           "couldn't write config to destination:%s" (Prelude.Url.show destination))
      >>= fun () ->
      Lwt_extra2.sleep_approx 60. >>= fun () ->
      if loop
      then inner ()
      else Lwt.return ()
  in
  inner ()


let _make_client albamgr_client_cfg
                ?fragment_cache
                ?manifest_cache_size
                ?bad_fragment_callback
                ?(albamgr_connection_pool_size = 10)
                ?nsm_host_connection_pool_size
                ?(osd_connection_pool_size = 10)
                ?(osd_timeout = 10.)
                ?(default_osd_priority = Osd.Low)
                ~tls_config
                ?release_resources
                ?(tcp_keepalive = Tcp_keepalive2.default)
                ?use_fadvise
                ?partial_osd_read
                ?cache_on_read
                ?cache_on_write
                ~upload_slack
                ~populate_osds_info_cache
                ?read_preference
                ()
  =
  let albamgr_pool =
    Remotes.Pool.Albamgr.make
      ~size:albamgr_connection_pool_size
      albamgr_client_cfg
      tls_config
      Buffer_pool.default_buffer_pool
      ~tcp_keepalive
  in
  let mgr_access = Albamgr_access.make albamgr_pool in
  let osd_access =
    new Osd_access.osd_access mgr_access
        ~osd_connection_pool_size ~osd_timeout
        ~default_osd_priority
        ~tls_config ~tcp_keepalive
        (Alba_osd.make_client
           ~albamgr_connection_pool_size
           ~upload_slack
           ~osd_connection_pool_size
           ~osd_timeout
           ~default_osd_priority
        )
  in
  Alba_client.make_client
    mgr_access
    ~osd_access
    ?fragment_cache
    ?manifest_cache_size
    ?bad_fragment_callback
    ?nsm_host_connection_pool_size
    ~tls_config
    ?release_resources
    ~tcp_keepalive
    ?use_fadvise
    ?partial_osd_read
    ?cache_on_read
    ?cache_on_write
    ~upload_slack
    ~populate_osds_info_cache
    ?read_preference
    ()

let make_client albamgr_client_cfg
                ?fragment_cache
                ?manifest_cache_size
                ?bad_fragment_callback
                ~albamgr_refresh_config
                ?albamgr_connection_pool_size
                ?nsm_host_connection_pool_size
                ?osd_connection_pool_size
                ?osd_timeout
                ?default_osd_priority
                ~tls_config
                ?release_resources
                ?tcp_keepalive
                ?use_fadvise
                ?partial_osd_read
                ?cache_on_read
                ?cache_on_write
                ~populate_osds_info_cache
                ~upload_slack
                ?read_preference
                ()
  =
  let client, closer =
    _make_client
      albamgr_client_cfg
      ?fragment_cache
      ?manifest_cache_size
      ?bad_fragment_callback
      ?albamgr_connection_pool_size
      ?nsm_host_connection_pool_size
      ?osd_connection_pool_size
      ?osd_timeout
      ?default_osd_priority
      ~tls_config
      ?release_resources
      ?tcp_keepalive
      ?use_fadvise
      ?partial_osd_read
      ?cache_on_read
      ?cache_on_write
      ~populate_osds_info_cache
      ~upload_slack
      ?read_preference
      ()
  in
  let () =
    match albamgr_refresh_config with
    | `RefreshFromAbmAndUpdate albamgr_cfg_url ->
       refresh_albamgr_cfg
          ~loop:true
          albamgr_client_cfg
          client
          albamgr_cfg_url
          ~tcp_keepalive
          ~tls_config
       |> Lwt.ignore_result
    | `RefreshFromConfig albamgr_cfg_url ->
       let rec inner () =
         Lwt.catch
           (fun () ->
             Alba_arakoon.config_from_url albamgr_cfg_url >>= fun abm_cfg ->
             let () = albamgr_client_cfg := abm_cfg in
             Lwt.return ()
           )
           (fun exn ->
             Lwt_log.info_f ~exn "Exception while refreshing albamgr cfg from disk"
           )
         >>= fun () ->
         Lwt_unix.sleep 60. >>= fun () ->
         inner ()
       in
       inner () |> Lwt.ignore_result
    | `None ->
       ()
  in
  client, closer

let with_client albamgr_client_cfg
                ?fragment_cache
                ?manifest_cache_size
                ?bad_fragment_callback
                ~albamgr_refresh_config
                ?albamgr_connection_pool_size
                ?nsm_host_connection_pool_size
                ?osd_connection_pool_size
                ?osd_timeout
                ?default_osd_priority
                ~tls_config
                ?release_resources
                ?tcp_keepalive
                ?use_fadvise
                ?partial_osd_read
                ?cache_on_read
                ?cache_on_write
                ?read_preference
                ~populate_osds_info_cache
                ~upload_slack
                f
  =
  let client, closer =
    make_client albamgr_client_cfg
                ?fragment_cache
                ?manifest_cache_size
                ?bad_fragment_callback
                ~albamgr_refresh_config
                ?albamgr_connection_pool_size
                ?nsm_host_connection_pool_size
                ?osd_connection_pool_size
                ?osd_timeout
                ?default_osd_priority
                ~tls_config
                ?release_resources
                ?tcp_keepalive
                ?use_fadvise
                ?partial_osd_read
                ?cache_on_read
                ?cache_on_write
                ~populate_osds_info_cache
                ~upload_slack
                ?read_preference
                ()
  in
  Lwt.finalize
    (fun () -> f client)
    closer
