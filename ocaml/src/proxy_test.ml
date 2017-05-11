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

open! Prelude
open Lwt.Infix
open Proxy_protocol.Protocol

let _IP,_PORT,_TRANSPORT =
  try
    let ip = Sys.getenv "ALBA_RDMA" in
    (ip, 10_000, Net_fd.RDMA)
  with
    Not_found ->
    ("127.0.0.1", 10_000, Net_fd.TCP)


let with_proxy_client f =
  Proxy_client.with_client
    _IP _PORT _TRANSPORT
    f

let test_with_proxy_client f =
  Test_extra.lwt_run (with_proxy_client f)

let assert_proxy_error f err =
  Lwt.catch
    (fun () ->
       f () >>= fun () ->
       Lwt.fail (Failure "Operation succeeded while it should have failed"))
    (function
      | Error.Exn (err', _) when err' = err ->
        Lwt.return ()
      | Error.Exn (err', _) ->
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
           (fun () -> Lwt_extra2.unlink ~fsync_parent_dir:false file_name)
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
       Networking2.connect_with
         ~tls_config:None
         _IP _PORT _TRANSPORT
       >>= fun (nfd, closer) ->
       Lwt.finalize
         (fun () ->
           Proxy_client._prologue nfd Protocol.magic 666l >>= fun () ->
           let session = ProxySession.make () in
           let client = new Proxy_client.proxy_client nfd session in
           client # get_version >>= fun _ ->
           OUnit.assert_bool "should have failed" false;
           Lwt.return ()
         )
         closer
      )
      (function
        | Protocol.Error.Exn (Protocol.Error.ProtocolVersionMismatch, _) ->
           Lwt_log.debug "ok, this is what we needed"
        | exn ->
           Lwt_log.info_f ~exn "wrong protocol error" >>= fun () ->
           Lwt.fail_with "wrong protocol error or no failure"
      )
  in
  Test_extra.lwt_run t

let test_unknown_operation () =
  test_with_proxy_client
    (fun client ->
     client # do_unknown_operation >>= fun () ->
     client # do_unknown_operation >>= fun () ->
     client # get_version >>= fun _ ->
     Lwt.return ())

let test_osd_view () =
  test_with_proxy_client
    (fun client ->
      client # osd_view >>= fun (claim, state_info) ->
      Lwt.return_unit)

open OUnit

let suite = "proxy_test" >:::[
    "test_delete_namespace" >:: test_delete_namespace;
    "test_create_namespace" >:: test_create_namespace;
    "test_overwrite" >:: test_overwrite;
    "test_read" >:: test_read;
    "test_protocol_version" >:: test_protocol_version;
    "test_unknown_operation" >:: test_unknown_operation;
    "test_osd_view" >:: test_osd_view;
  ]
