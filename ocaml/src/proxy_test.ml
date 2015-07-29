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

open Lwt
open Proxy_protocol.Protocol

let _IP = "::1"
let _PORT = 10_000

let test_with_proxy_client f =
  let t =
    Proxy_client.with_client
      _IP _PORT
      f
  in
  Lwt_main.run t

let assert_proxy_error f err =
  Lwt.catch
    (fun () ->
       f () >>= fun () ->
       Lwt.fail (Failure "Operation succeeded while it should have failed"))
    (function
      | Error.Exn err' when err' = err ->
        Lwt.return ()
      | Error.Exn err' ->
        Lwt.fail
          (Failure
             (Printf.sprintf
                "Received wrong error code %s while %s expected"
                (Error.show err')
                (Error.show err)))
      | exn ->
        Lwt.fail exn)


let test_delete_namespace () =
  test_with_proxy_client
    (fun client ->
       let name = "test_delete_namespace" in
       Lwt_io.printlf "1" >>= fun () ->
       client # create_namespace
              ~namespace:name  ~preset_name:None >>= fun () ->
       Lwt_io.printlf "2" >>= fun () ->
       client # delete_namespace
         ~namespace:name >>= fun () ->
       Lwt_io.printlf "3" >>= fun () ->
       assert_proxy_error
         (fun () ->
            client # delete_namespace ~namespace:name)
         Error.NamespaceDoesNotExist)


let test_create_namespace () =
  test_with_proxy_client
    (fun client ->
       let name = "test_create_namespace" in
       Lwt_io.printlf "1" >>= fun () ->
       client # create_namespace
         ~namespace:name ~preset_name:None >>= fun () ->
       Lwt_io.printlf "2" >>= fun () ->
       assert_proxy_error
         (fun () ->
            client # create_namespace ~namespace:name ~preset_name:None)
         Error.NamespaceAlreadyExists)

let input_file_name () =
  Filename.concat (Sys.getcwd ()) "ocaml/myocamlbuild.ml"


let test_overwrite () =
  test_with_proxy_client
    (fun client ->
       let namespace = "test_overwrite" in
       let object_name = "test" in
       let input_file = input_file_name () in

       client # create_namespace ~namespace ~preset_name:None >>= fun () ->

       let write_file () =
         let allow_overwrite = false in
         client # write_object_fs
           ~namespace
           ~object_name
           ~input_file
           ~allow_overwrite () in
       write_file () >>= fun () ->

       assert_proxy_error
         write_file
         Error.OverwriteNotAllowed)


let test_read () =
  test_with_proxy_client
    (fun client ->
       let namespace = "test_read" in
       client # create_namespace ~namespace ~preset_name:None >>= fun () ->

       let object_name = "test" in
       let input_file = input_file_name() in
       client # write_object_fs
         ~namespace
         ~object_name
         ~input_file
         ~allow_overwrite:true ()
       >>= fun () ->

       let safe_unlink file_name =
         Lwt.catch
           (fun () -> Lwt_unix.unlink file_name)
           (function
             | Unix.Unix_error (Unix.ENOENT, _, _) -> Lwt.return ()
             | exn -> Lwt.fail exn)
       in

       let file_name = "/tmp/alba_test_read" in
       safe_unlink file_name >>= fun () ->
       client # read_object_fs
         ~namespace
         ~object_name
         ~output_file:file_name
         ~consistent_read:true
         ~should_cache:true
       >>= fun () ->

       let file_should_not_be_written = "/tmp/does_not_exist" in
       safe_unlink file_should_not_be_written >>= fun () ->
       assert_proxy_error
         (fun () ->
            client # read_object_fs
              ~namespace
              ~object_name:"fdsjakf;sd does not exist"
              ~output_file:file_should_not_be_written
              ~consistent_read:true
              ~should_cache:false
         )
         Error.ObjectDoesNotExist >>= fun () ->

       let exists filename =
         Lwt.catch
           (fun () ->
              Lwt_unix.stat filename >>= fun _ ->
              Lwt.return true)
           (function
             | Unix.Unix_error (Unix.ENOENT,_,_) -> Lwt.return false
             | e -> Lwt.fail e
           ) in

       exists file_should_not_be_written >>= fun exists ->
       assert (not exists);
       Lwt.return ())


let test_protocol_version () =
  let t =
    let open Proxy_protocol in
    Lwt.catch
    (fun () ->
     Lwt_io.with_connection
       (Networking2.make_address _IP _PORT)
       (fun conn ->
        let oc = snd conn in
        Proxy_client._prologue oc Protocol.magic 666l
        >>= fun () ->
        let client = new Proxy_client.proxy_client conn in
        client # get_version >>= fun _ ->
        OUnit.assert_bool "should have failed" false;
        Lwt.return ()
       )
    )
    (function
      | Protocol.Error.Exn Protocol.Error.ProtocolVersionMismatch ->
         Lwt_log.debug "ok, this is what we needed"
      | exn ->
         Lwt_log.info_f ~exn "wrong protocol error" >>= fun () ->
         Lwt.fail_with "wrong protocol error or no failure"
    )
  in
  Lwt_main.run t

open OUnit

let suite = "proxy_test" >:::[
    "test_delete_namespace" >:: test_delete_namespace;
    "test_create_namespace" >:: test_create_namespace;
    "test_overwrite" >:: test_overwrite;
    "test_read" >:: test_read;
    "test_protocol_version" >:: test_protocol_version;
  ]
