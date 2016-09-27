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

let test_with_kvs_client
      test_name
      f =
  Alba_test.test_with_alba_client
    (fun alba_client ->
     alba_client # mgr_access # get_alba_id >>= fun alba_id ->

     let prefix = "alba_osd_test/" ^ test_name in
     alba_client # create_namespace ~namespace:prefix ~preset_name:None () >>= fun _ ->
     let client = new Alba_osd.client
                      alba_client
                      ~alba_id
                      ~prefix ~preset_name:None
                      ~namespace_name_format:1
     in
     f (client # global_kvs))

let test_set_get_delete () =
  test_with_kvs_client
    "test_set_get_delete"
    (Osd_kvs_test.test_set_get_delete ~verify_value:true)

let test_multiget () =
  test_with_kvs_client
    "test_multiget"
    Osd_kvs_test.test_multiget

let test_multi_exists () =
  test_with_kvs_client
    "test_multi_exists"
    Osd_kvs_test.test_multi_exists

let test_range_query () =
  test_with_kvs_client
    "test_range_query"
    Osd_kvs_test.test_range_query

let test_delete () =
  test_with_kvs_client
    "test_delete"
    Osd_kvs_test.test_delete

let test_list_all () =
  test_with_kvs_client
    "test_list_all"
    Osd_kvs_test.test_list_all

let test_assert () =
  test_with_kvs_client
    "test_assert"
    Osd_kvs_test.test_assert

let test_partial_get () =
  test_with_kvs_client
    "test_partial_get"
    Osd_kvs_test.test_partial_get

let test_multi_update_for_same_key () =
  test_with_kvs_client
    "test_multi_update_for_same_key"
    Osd_kvs_test.test_multi_update_for_same_key

let test_update_abm_cfg () =
  Albamgr_test.test_with_albamgr
    (fun client ->
     let long_id = get_random_string 32 in
     client # add_osd
            Nsm_model.OsdInfo.(
       {
         kind = Alba2 { id = long_id;
                        cfg = Arakoon_client_config.(
                          { cluster_id = "cluster id";
                            node_cfgs = [];
                            ssl_cfg = None;
                            tcp_keepalive = Tcp_keepalive.default_tcp_keepalive
                          });
                        prefix = "";
                        preset = ""; };
         decommissioned = false;
         node_id = "";
         other = "";
         total = 0L; used = 0L;
         seen = [];
         read = [];
         write = [];
         errors = [];
       }) >>= fun () ->
     client # get_osd_by_long_id ~long_id >>= fun o_osd_info ->

     client # update_osds
            [ (long_id,
               Albamgr_protocol.Protocol.Osd.Update.make
                 ~albamgr_cfg':Arakoon_client_config.(
                 { cluster_id = "cluster id";
                   node_cfgs = [ "fdsa", { ips = []; port=3; } ];
                   ssl_cfg = None;
                   tcp_keepalive = Tcp_keepalive.default_tcp_keepalive;
                 })
                                                    ()
              ) ] >>= fun () ->

     client # get_osd_by_long_id ~long_id >>= fun o_osd_info2 ->
     assert (o_osd_info <> o_osd_info2);
     let open Nsm_model.OsdInfo in
     (match (snd (Option.get_some o_osd_info2)).kind with
      | Alba2 { cfg; _; } ->
         let open Arakoon_client_config in
         assert (1 = List.length cfg.node_cfgs);
         assert ("cluster id" = cfg.cluster_id);
         Lwt.return ()
      | _ ->
         assert false) >>= fun () ->

     (* cluster id isn't allowed to change, so this should throw *)
     Lwt.catch
       (fun () ->
        client # update_osds
               [ (long_id,
                  Albamgr_protocol.Protocol.Osd.Update.make
                    ~albamgr_cfg':Arakoon_client_config.(
                    { cluster_id = "new cluster id??";
                      node_cfgs = [];
                      ssl_cfg = None;
                      tcp_keepalive = Tcp_keepalive.default_tcp_keepalive;
                    })
                                                       ()
                 ) ] >>= fun () ->
        Lwt.return `Didnt_throw)
       (fun exn ->
        Lwt.return `Threw) >>= fun res ->
     assert (res = `Threw);
     Lwt.return ()
    )

open OUnit

let suite = "alba_osd_test" >:::[
    "test_set_get_delete" >:: test_set_get_delete;
    "test_multiget" >:: test_multiget;
    "test_range_query" >:: test_range_query;
    "test_delete" >:: test_delete;
    "test_list_all" >:: test_list_all;
    "test_multi_exists" >:: test_multi_exists;
    "test_assert" >:: test_assert;
    "test_partial_get" >:: test_partial_get;
    "test_multi_update_for_same_key" >:: test_multi_update_for_same_key;
    "test_update_abm_cfg" >:: test_update_abm_cfg;
  ]
