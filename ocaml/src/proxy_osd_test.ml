(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Proxy_osd
open Lwt.Infix

let test_with_kvs_client test_name f =
  let t () =
    Lwt_log.debug_f "Starting proxy_osd_test %s" test_name >>= fun () ->
    let ip, port, transport =
      let open Proxy_test in
      _IP, _PORT, _TRANSPORT
    in
    let pp = new Proxy_osd.simple_proxy_pool ~ip ~port ~transport ~size:1 in
    Lwt.finalize
      (fun () ->
        let prefix = "proxy_osd_test/" ^ test_name in
        let proxy_osd = new t
                            (pp :> Proxy_osd.proxy_pool)
                            ~alba_id:"long_id"
                            ~prefix ~preset:"default"
                            ~namespace_name_format:1
                            ~version:Alba_version.summary
        in
        pp # with_client
           ~namespace:""
           (fun c -> c # create_namespace ~namespace:prefix ~preset_name:None) >>= fun () ->
        f (proxy_osd # global_kvs))
      (fun () -> pp # finalize)
  in
  Test_extra.lwt_run (t ())

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

open OUnit

let suite = "proxy_osd_test" >:::[
      "test_set_get_delete" >:: test_set_get_delete;
      "test_multiget" >:: test_multiget;
      "test_multi_exists" >:: test_multi_exists;
      "test_range_query" >:: test_range_query;
      "test_delete" >:: test_delete;
      "test_list_all" >:: test_list_all;
      "test_assert" >:: test_assert;
      "test_partial_get" >:: test_partial_get;
      "test_multi_update_for_same_key" >:: test_multi_update_for_same_key;
    ]
