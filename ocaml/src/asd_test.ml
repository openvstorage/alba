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
open Osd
open Lwt.Infix
open Slice

let rec wait_asd_connection port asd_id () =
  Lwt.catch
    (fun () ->
     let tls_config = Albamgr_test.get_tls_config () in
     let conn_info = Networking2.make_conn_info [ "127.0.0.1" ] port tls_config in
     Asd_client.with_client
       ~conn_info  asd_id
       (fun client -> Lwt.return `Continue))
    (function
      | exn when Networking2.is_connection_failure_exn exn ->
         Lwt.return `Retry
      | exn ->
         Lwt.fail exn) >>= function
  | `Continue ->
     Lwt.return ()
  | `Retry ->
     Lwt_unix.sleep 0.1 >>= fun () ->
     wait_asd_connection port asd_id ()

let workspace =
  try Sys.getenv "WORKSPACE"
  with Not_found -> ""

let capacity = ref 0L

let with_asd_client ?(is_restart=false) ?write_blobs test_name port f =
  let path = workspace ^ "/tmp/alba/" ^ test_name in
  let abm_tls_config = Albamgr_test.get_tls_config () in (* client config *)
  let o_port, tls_config =
    match abm_tls_config
    with
    | None -> Some port, None
    | Some _ ->
       let path = workspace ^ "/tmp/arakoon/test_discover_claimed/" in
       let open Asd_config.Config in
       None,
       Some {cert = path ^ "test_discover_claimed.pem";
             key  = path ^ "test_discover_claimed.key";
             port;
            }
  in
  if not is_restart
  then
    begin
      capacity := 10_000_000L;
      Unix.system (Printf.sprintf "rm -rf %s" path) |> ignore;
      Unix.system (Printf.sprintf "mkdir -p %s" path) |> ignore
    end;
  let asd_id = Some test_name in
  let cancel = Lwt_condition.create () in
  let t =
    Lwt.pick
      [ (Asd_server.run_server
           ?write_blobs
           ~cancel
           ~tcp_keepalive:Tcp_keepalive2.default
           [] ~port:o_port ~rora_port:None
           ~transport:Net_fd.TCP
           path
           ~asd_id
           ~node_id:"bla"
           ~slow:false
           ~fsync:false
           ~rocksdb_max_open_files:256
           ~rocksdb_recycle_log_file_num:None
           ~rocksdb_block_cache_size:None
           ~limit:90L
           ~tls_config
           ~capacity
           ~multicast:(Some 10.0)
           ~use_fadvise:true
           ~use_fallocate:true
         >>= fun () ->
         Lwt.fail_with "Asd server stopped!");
        begin
          Lwt_unix.with_timeout
            5.
            (wait_asd_connection port asd_id)>>= fun () ->
          let conn_info = Networking2.make_conn_info [ "127.0.0.1" ] port abm_tls_config in
          Asd_client.with_client
            ~conn_info (Some test_name)
            f >>= fun r ->
          Lwt_condition.broadcast cancel ();
          Lwt.return r
        end; ]
  in
  t

let test_with_asd_client ?write_blobs test_name port f =
  Lwt_main.run (with_asd_client ?write_blobs test_name port f)

let test_with_kvs_client ?write_blobs test_name port f =
  Lwt_main.run
    begin
      with_asd_client
        ?write_blobs
        test_name
        port
        (fun asd -> f ((new Asd_client.asd_osd test_name asd) # kvs))
    end

let test_set_get_delete port () =
  test_with_kvs_client
    "test_set_get_delete" port
    (Osd_kvs_test.test_set_get_delete ~verify_value:true)

let test_no_blobs port () =
  test_with_kvs_client
    ~write_blobs:false
    "test_no_blobs" port
    (Osd_kvs_test.test_set_get_delete ~verify_value:false)

let test_multiget port () =
  test_with_kvs_client
    "test_multiget" port
    Osd_kvs_test.test_multiget

let test_multi_exists port () =
  test_with_kvs_client
    "test_multi_exists" port
    Osd_kvs_test.test_multi_exists

let test_range_query port () =
  test_with_kvs_client
    "test_range_query" port
    Osd_kvs_test.test_range_query

let test_delete port () =
  test_with_kvs_client
    "test_delete" port
    Osd_kvs_test.test_delete

let test_list_all port () =
  test_with_kvs_client
    "test_list_all" port
    Osd_kvs_test.test_list_all

let test_assert port () =
  test_with_kvs_client
    "test_assert" port
    Osd_kvs_test.test_assert

let test_partial_get port () =
  test_with_kvs_client
    "test_partial_get" port
    Osd_kvs_test.test_partial_get

let test_multi_update_for_same_key port () =
  test_with_kvs_client
    "test_multi_update_for_same_key" port
    Osd_kvs_test.test_multi_update_for_same_key


let test_startup port1 port2 () =
  let tn = "test_startup" in
  let size = Asd_server.blob_threshold + 2 in
  let v1 = Bytes.make size 'a' in
  let v2 = Bytes.make size 'b' in
  let v3 = Bytes.make size 'c' in
  let v4 = Bytes.make size 'd' in
  let t =
    with_asd_client
      tn port1
      (fun client ->
         client # set_string ~prio:High "a" v1 true >>= fun () ->
         client # set_string ~prio:High "b" v2 true >>= fun () ->
         Lwt.return ()) >>= fun () ->
    Lwt_unix.sleep 0.3 >>= fun () ->
    (* TODO shutdown in a not gracefull way! hmm, think about this. *)
    with_asd_client
      ~is_restart:true
      tn port2
      (fun client ->
         client # set_string ~prio:High "c" v3 true >>= fun () ->
         client # set_string ~prio:High "d" v4 true >>= fun () ->

         client # multi_get_string ~prio:High ["a"; "b"; "c"; "d"; ] >>= fun vs ->
         let vs' = List.map (fun v -> fst (Option.get_some v)) vs in
         assert (vs' = [ v1; v2; v3; v4 ]);
         Lwt.return ())
  in
  Lwt_main.run t

let test_protocol_version port () =
  let protocol_test port =
    Lwt.catch
      (fun () ->
       let tls_config = Albamgr_test.get_tls_config () in
       let conn_info = Networking2.make_conn_info ["127.0.0.1"] port tls_config in
       Networking2.first_connection ~conn_info
       >>= fun (fd, closer) ->
       Lwt.finalize
         (fun () ->
          let prologue_bytes = Asd_client.make_prologue
                                 Asd_protocol._MAGIC 666l None in
          Net_fd.write_all' fd prologue_bytes >>= fun () ->
          Asd_client._prologue_response fd None >>= fun _ ->
          OUnit.assert_bool "should have failed" false;
          Lwt.return ())
         closer
      )
      ( let open Asd_protocol in
        function
        | Protocol.Error.Exn (Protocol.Error.ProtocolVersionMismatch _) -> Lwt.return ()
        | exn ->
           OUnit.assert_bool "wrong exn" false;
           Lwt.return ()
      )
      >>= fun () ->
    Lwt_log.debug "ok"
  in
  let test_name = "test_protocol_version" in
  test_with_asd_client
    test_name port
    (fun _client ->
     protocol_test port)

let test_unknown_operation port () =
  test_with_asd_client
    "test_unknown_operation" port
    (fun asd ->
     asd # do_unknown_operation >>= fun () ->
     asd # do_unknown_operation >>= fun () ->
     asd # multi_get ~prio:Asd_protocol.Protocol.High [ Slice.wrap_string "x" ] >>= fun _ ->
     Lwt.return ()
    )

let test_capacity port () =
  let test_name = "test_capacity" in
  let t () =
    with_asd_client
      test_name port
      (fun asd ->

       asd # get_disk_usage () >>= fun (used, cap) ->
       assert (used = 0L);
       assert (cap = 10_000_000L);

       capacity := 1_000_000L;
       asd # get_disk_usage () >>= fun (used, cap) ->
       assert (used = 0L);
       assert (cap = 1_000_000L);

       let v = Bytes.make 999_999 'a' in
       asd # set_string ~prio:High "k" v true >>= fun () ->

       Lwt.catch
         (fun () ->
          asd # set_string ~prio:High "k2" v true >>= fun () ->
          assert false)
         (function
           | Asd_protocol.Protocol.Error.Exn Asd_protocol.Protocol.Error.Full ->
              Lwt.return_unit
           | exn ->
              Lwt.fail exn) >>= fun () ->

       asd # get_disk_usage ())
    >>= fun (used, cap) ->
    assert (used > 0L);
    with_asd_client
      ~is_restart:true
      test_name port
      (fun asd ->
       asd # get_disk_usage () >>= fun (used', cap') ->
       Lwt_log.debug_f "%Li, %Li" used' cap' >>= fun () ->
       assert (used = used');
       assert (cap = cap');

       asd # delete_string ~prio:High "k" >>= fun () ->
       asd # get_disk_usage () >>= fun (used, cap) ->
       assert (used = 0L);
       Lwt.return ())
  in
  Lwt_main.run (t ())

open OUnit

let suite = "asd_test" >:::[
    "test_set_get_delete" >:: test_set_get_delete 7900;
    "test_multiget" >:: test_multiget 7901;
    "test_range_query" >:: test_range_query 7902;
    "test_delete" >:: test_delete 7903;
    "test_list_all" >:: test_list_all 7904;
    "test_startup" >:: test_startup 7905 7906;
    "test_protocol_version" >:: test_protocol_version 7907;
    "test_multi_exists" >:: test_multi_exists 7908;
    "test_unknown_operation" >:: test_unknown_operation 7909;
    "test_no_blobs" >:: test_no_blobs 7910;
    "test_assert" >:: test_assert 7911;
    "test_partial_get" >:: test_partial_get 7911;
    "test_capacity" >:: test_capacity 7912;
    "test_multi_update_for_same_key" >:: test_multi_update_for_same_key 7913;
  ]
