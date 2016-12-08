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

let make_client albamgr_client_cfg
                ?fragment_cache
                ?manifest_cache_size
                ?bad_fragment_callback
                ?(albamgr_connection_pool_size = 10)
                ?nsm_host_connection_pool_size
                ?(osd_connection_pool_size = 10)
                ?(osd_timeout = 2.)
                ?(default_osd_priority = Osd.Low)
                ~tls_config
                ?release_resources
                ?(tcp_keepalive = Tcp_keepalive2.default)
                ?use_fadvise
                ?partial_osd_read
                ?cache_on_read
                ?cache_on_write
                ~upload_slack
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
        (Alba_osd.make_client ~albamgr_connection_pool_size ~upload_slack)
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
    ?read_preference
    ()

let with_client albamgr_client_cfg
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
