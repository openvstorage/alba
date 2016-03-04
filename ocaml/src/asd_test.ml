(*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

open Prelude
open Osd
open Lwt.Infix

let buffer_pool = Buffer_pool.osd_buffer_pool

let rec wait_asd_connection port asd_id () =
  Lwt.catch
    (fun () ->
     let tls_config = Albamgr_test.get_tls_config () in
     let conn_info = Networking2.make_conn_info [ "127.0.0.1" ] port tls_config in
     Asd_client.with_client
       buffer_pool
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

let with_asd_client ?(is_restart=false) ?write_blobs test_name port f =
  let path = workspace ^ "/tmp/alba/" ^ test_name in
  let tls_config = Albamgr_test.get_tls_config () in (* client config *)
  let o_port, tls =
    match tls_config
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
  then begin
    Unix.system (Printf.sprintf "rm -rf %s" path) |> ignore;
    Unix.mkdir path 0o777
  end;
  let asd_id = Some test_name in
  let cancel = Lwt_condition.create () in
  let t =
    Lwt.pick
      [ (Asd_server.run_server
           ?write_blobs
           ~cancel
           ~tcp_keepalive:Tcp_keepalive2.default
           [] o_port path
           ~asd_id
           ~node_id:"bla"
           ~slow:false
           ~fsync:false
           ~buffer_size:(768*1024)
           ~rocksdb_max_open_files:256
           ~limit:90L
           ~tls
           ~multicast:(Some 10.0)
           ~use_fadvise:true
           ~use_fallocate:true
         >>= fun () ->
         Lwt.fail_with "Asd server stopped!");
        begin
          Lwt_unix.with_timeout
            5.
            (wait_asd_connection port asd_id)>>= fun () ->
          let conn_info = Networking2.make_conn_info [ "127.0.0.1" ] port tls_config in
          Asd_client.with_client
            buffer_pool
            ~conn_info (Some test_name)
            f >>= fun r ->
          Lwt_condition.broadcast cancel ();
          Lwt.return r
        end; ]
  in
  t

let test_with_asd_client ?write_blobs test_name port f =
  Lwt_main.run (with_asd_client ?write_blobs test_name port f)


let test_set_get_delete ~verify_value (client : Asd_client.client) =
  let key = "key" in
  let size = Asd_server.blob_threshold + 2 in
  let value = Bytes.make size 'a' in
  client # set_string ~prio:High key value true >>= fun () ->
  client # get_string ~prio:High key >>= fun v_o ->
  (match v_o with
   | None -> failwith "oops got None"
   | Some (v, _) -> if verify_value
                    then assert (v = value));
  client # delete_string ~prio:High key

let test_set_get port () =
  test_with_asd_client
    "test_set_get" port
    (test_set_get_delete ~verify_value:true)

let test_multiget port () =
  test_with_asd_client
    "test_multiget" port
    (fun client ->
       let value1 = "fdidid" in
       let value2 = "vasi" in
       client # set_string ~prio:High "key"  value1 true >>= fun () ->
       client # set_string ~prio:High "key2" value2 true >>= fun () ->
       client # multi_get_string ~prio:High ["key"; "key2"] >>= fun res ->
       assert (2 = List.length res);
       assert ([ Some value1; Some value2; ] = List.map (Option.map fst) res);
       Lwt.return ())

let test_multi_exists port () =
  test_with_asd_client
    "test_multi_exists" port
    (fun client ->
     let open Slice in
     let v = "xxxx" in
     let existing_key = "exists" in
     client # set_string ~prio:High existing_key v true >>= fun () ->
     client # multi_exists ~prio:High [
              Slice.wrap_string existing_key;
              Slice.wrap_string "non_existing"
            ]
     >>= fun res ->
     assert (2 = List.length res);
     assert ([true;false] = res );
     Lwt.return ()
    )

let test_range_query port () =
  test_with_asd_client
    "test_range_query" port
    (fun client ->
       let v = "" in
       let set k = Osd.Update.set_string k v Checksum.Checksum.NoChecksum
                                         false
       in
       client # apply_sequence ~prio:High
         []
         [ set "" ; set "k"; set "kg"; set "l"; ] >>= fun () ->

       client # range_string ~prio:High
         ~first:"" ~finc:true ~last:None
         ~max:(-1) ~reverse:false >>= fun ((cnt, keys), _) ->

       Lwt_io.printlf
         "Found the following keys: %s"
         ([%show : string list] keys) >>= fun () ->
       assert (4 = cnt);
       assert (keys= [""; "k"; "kg"; "l";]);

       client # range_string ~prio:High
         ~first:"l" ~finc:true ~last:(Some("o", false))
         ~max:(-1) ~reverse:false >>= fun ((cnt, keys), _) ->

       Lwt_io.printlf
         "Found the following keys: %s"
         ([%show : string list] keys) >>= fun () ->
       assert (1 = cnt);
       assert (keys = ["l";]);
       Lwt.return ())

let test_delete port () =
  test_with_asd_client
    "test_delete" port
    (fun client ->
       let key = "sda" in
       client # delete_string ~prio:High key >>= fun () ->
       client # get_string ~prio:High key >>= fun res ->
       assert (None = res);
       client # set_string ~prio:High key key false >>= fun () ->
       client # get_string ~prio:High key >>= fun res ->
       assert (None <> res);
       client # delete_string ~prio:High key >>= fun () ->
       client # get_string ~prio:High key >>= fun res ->
       assert (None = res);
       Lwt.return ())

let test_list_all port () =
  test_with_asd_client
    "test_list_all" port
    (fun client ->
       let rec add_keys = function
         | 100 -> Lwt.return ()
         | n ->
           client # set_string ~prio:High (string_of_int n) "x" false >>= fun () ->
           add_keys (n + 1)
       in
       add_keys 0 >>= fun () ->

       client # range_all ~prio:High ~max:50 () >>= fun (cnt, _) ->
       Lwt_log.debug_f "cnt = %i" cnt >>= fun () ->
       assert (cnt = 100);

       client # range_all ~prio:High ~max:99 () >>= fun (cnt, _) ->
       Lwt_log.debug_f "cnt = %i" cnt >>= fun () ->
       assert (cnt = 100);

       client # range_all ~prio:High ~max:(-1) () >>= fun (cnt, _) ->
       assert (cnt = 100);

       client # range_all ~prio:High ~max:100 () >>= fun (cnt, _) ->
       assert (cnt = 100);

       client # range_all ~prio:High ~max:49 () >>= fun (cnt, _) ->
       assert (cnt = 100);

       Lwt.return ())

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
          Net_fd.write_all prologue_bytes fd >>= fun () ->
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
     asd # multi_get ~prio:Asd_protocol.Protocol.High [ Slice.Slice.wrap_string "x" ] >>= fun _ ->
     Lwt.return ()
    )

let test_no_blobs port () =
  test_with_asd_client
    ~write_blobs:false
    "test_no_blobs" port
    (test_set_get_delete ~verify_value:false)

let test_assert port () =
  test_with_asd_client
    "test_assert" port
    (fun asd ->
     Lwt.catch
       (fun () ->
        asd # apply_sequence ~prio:High
            [ Osd.Assert.value_string "key" "value"; ]
            [ Osd.Update.set_string
                "key"
                (Bytes.create (Asd_server.blob_threshold + 2))
                Checksum.Checksum.NoChecksum false; ])
       (function
         | Asd_protocol.Protocol.Error.Exn Asd_protocol.Protocol.Error.Assert_failed _ ->
            Lwt.return_unit
         | exn ->
            Lwt.fail exn)
     >>= fun () ->
     asd # apply_sequence ~prio:High
         []
         [ Osd.Update.set_string
             "key"
             (Bytes.create (Asd_server.blob_threshold + 2))
             Checksum.Checksum.NoChecksum false; ] >>= fun () ->
     asd # multi_get_string ~prio:High [ "x" ] >>= fun _ ->
     Lwt.return ())

open OUnit

let suite = "asd_test" >:::[
    "test_set_get" >:: test_set_get 7900;
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
  ]
