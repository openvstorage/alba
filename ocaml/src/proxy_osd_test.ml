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

open Proxy_osd
open Lwt.Infix

let test_with_kvs_client prefix f =
  Proxy_test.test_with_proxy_client
    (fun proxy_client ->
      let proxy_osd = new t
                          proxy_client
                          ~long_id:"long_id"
                          ~prefix ~preset_name:None
                          ~namespace_name_format:1
      in
      proxy_client # create_namespace ~namespace:prefix ~preset_name:None >>= fun () ->
      f (proxy_osd # global_kvs))

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
