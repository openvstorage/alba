(*
Copyright 2015 Open vStorage NV

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

open Lwt.Infix
open Prelude

let buffer_pool = Buffer_pool.osd_buffer_pool

let rec wait_asd_connection port asd_id () =
  Lwt.catch
    (fun () ->
     Asd_client.with_client
       buffer_pool
       [ "::1" ] port asd_id
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

let with_asd_client ?(is_restart=false) test_name port f =
  let path = "/tmp/alba/" ^ test_name in
  if not is_restart
  then begin
    Unix.system (Printf.sprintf "rm -rf %s" path) |> ignore;
    Unix.mkdir path 0o777
  end;
  let asd_id = Some test_name in
  let t =
    Lwt.pick
      [ Asd_server.run_server
          [] port path
          ~asd_id
          ~node_id:"bla"
          ~slow:false
          ~fsync:false
          ~buffer_size:(768*1024)
          ~rocksdb_max_open_files:256
          ~limit:90L
          ~multicast:(Some 10.0);
        begin
          Lwt_unix.with_timeout
            5.
            (wait_asd_connection port asd_id)>>= fun () ->
          Asd_client.with_client
            buffer_pool
            [ "::1" ] port (Some test_name)
            f
        end; ]
  in
  t

let test_with_asd_client test_name port f =
  Lwt_main.run (with_asd_client test_name port f)


let test_set_get port () =
  test_with_asd_client
    "test_set_get" port
    (fun client ->
       let key = "key" in
       let value = "value" in
       client # set_string key value true >>= fun () ->
       client # get_string key >>= function
       | None -> failwith "oops got None"
       | Some (v, _) ->
         assert (v = value);
         Lwt_io.printlf "Got %s" v)

let test_multiget port () =
  test_with_asd_client
    "test_multiget" port
    (fun client ->
       let value1 = "fdidid" in
       let value2 = "vasi" in
       client # set_string "key"  value1 true >>= fun () ->
       client # set_string "key2" value2 true >>= fun () ->
       client # multi_get_string ["key"; "key2"] >>= fun res ->
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
     client # set_string existing_key v true >>= fun () ->
     client # multi_exists [
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
       client # apply_sequence
         []
         [ set "" ; set "k"; set "kg"; set "l"; ] >>= fun _ ->

       client # range_string
         ~first:"" ~finc:true ~last:None
         ~max:(-1) ~reverse:false >>= fun ((cnt, keys), _) ->

       Lwt_io.printlf
         "Found the following keys: %s"
         ([%show : string list] keys) >>= fun () ->
       assert (4 = cnt);
       assert (keys= [""; "k"; "kg"; "l";]);

       client # range_string
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
       client # delete_string key >>= fun () ->
       client # get_string key >>= fun res ->
       assert (None = res);
       client # set_string key key false >>= fun () ->
       client # get_string key >>= fun res ->
       assert (None <> res);
       client # delete_string key >>= fun () ->
       client # get_string key >>= fun res ->
       assert (None = res);
       Lwt.return ())

let test_list_all port () =
  test_with_asd_client
    "test_list_all" port
    (fun client ->
       let rec add_keys = function
         | 100 -> Lwt.return ()
         | n ->
           client # set_string (string_of_int n) "x" false >>= fun () ->
           add_keys (n + 1)
       in
       add_keys 0 >>= fun () ->

       client # range_all ~max:50 () >>= fun (cnt, _) ->
       Lwt_log.debug_f "cnt = %i" cnt >>= fun () ->
       assert (cnt = 100);

       client # range_all ~max:99 () >>= fun (cnt, _) ->
       Lwt_log.debug_f "cnt = %i" cnt >>= fun () ->
       assert (cnt = 100);

       client # range_all ~max:(-1) () >>= fun (cnt, _) ->
       assert (cnt = 100);

       client # range_all ~max:100 () >>= fun (cnt, _) ->
       assert (cnt = 100);

       client # range_all ~max:49 () >>= fun (cnt, _) ->
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
         client # set_string "a" v1 true >>= fun () ->
         client # set_string "b" v2 true >>= fun () ->
         Lwt.return ()) >>= fun () ->
    Lwt_unix.sleep 0.3 >>= fun () ->
    (* TODO shutdown in a not gracefull way! hmm, think about this. *)
    with_asd_client
      ~is_restart:true
      tn port2
      (fun client ->
         client # set_string "c" v3 true >>= fun () ->
         client # set_string "d" v4 true >>= fun () ->

         client # multi_get_string ["a"; "b"; "c"; "d"; ] >>= fun vs ->
         let vs' = List.map (fun v -> fst (Option.get_some v)) vs in
         assert (vs' = [ v1; v2; v3; v4 ]);
         Lwt.return ())
  in
  Lwt_main.run t

let test_protocol_version port () =
  let test_name = "test_protocol_version" in
  let path = "/tmp/alba/" ^ test_name in
  Unix.system (Printf.sprintf "rm -rf %s" path) |> ignore;
  Unix.mkdir path 0o777;
  let asd_id = Some test_name in
  let protocol_test port =
    Lwt.catch
      (fun () ->
       Networking2.first_connection' buffer_pool ["::1"] port
       >>= fun (_, (ic,oc), closer) ->
       Lwt.finalize
         (fun () ->
          let prologue_bytes = Asd_client.make_prologue
                                 Asd_protocol._MAGIC 666l None in
          Lwt_io.write oc prologue_bytes >>= fun () ->
          Asd_client._prologue_response ic None >>= fun _ ->
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
  let t =
    Lwt.pick [
        Asd_server.run_server
          [] port path
          ~asd_id
          ~node_id:"node"
          ~slow:false
          ~fsync:false
          ~buffer_size:(768*1024)
          ~rocksdb_max_open_files:256
          ~limit:90L
          ~multicast:(Some 10.0);
        Lwt_unix.with_timeout
          5.
          (wait_asd_connection port asd_id)
        >>= fun () ->
        protocol_test port
      ]
  in
  Lwt_main.run t


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
  ]
