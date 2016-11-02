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
open Proxy_protocol
open Protocol
open Range_query_args


class proxy_client fd =
  let with_response deserializer f =
    let module Llio = Llio2.NetFdReader in
    Llio.int_from fd >>= fun size ->
    Llio.with_buffer_from
      fd size
      (fun res_buf ->
       match Llio2.ReadBuffer.int_from res_buf with
       | 0 ->
          f (deserializer res_buf)
       | err ->
          let err_string = Llio2.ReadBuffer.string_from res_buf in
          Lwt_log.debug_f "Proxy client received error from server: %s" err_string
          >>= fun () -> Error.failwith ~payload:err_string (Error.int2err err))
  in
  let do_request code serialize_request request response_deserializer f =
    let module Llio = Llio2.WriteBuffer in
    let buf =
      Llio.serialize_with_length'
        (Llio.pair_to
           Llio.int_to
           serialize_request)
        (code,
         request)
    in

    Net_fd.write_all_lwt_bytes
      fd buf.Llio.buf 0 buf.Llio.pos
    >>= fun () ->

    with_response response_deserializer f
  in
  object(self)
    method private request' : type i o r. (i, o) request -> i -> (o -> r Lwt.t) -> r Lwt.t =
      fun command req f ->
      do_request
        (command_to_code (Wrap command))
        (deser_request_i command |> snd) req
        (Deser.from_buffer (deser_request_o command))
        f

    method private request : type i o. (i, o) request -> i -> o Lwt.t =
      fun command req ->
      self # request' command req Lwt.return

    method do_unknown_operation =
      let code =
        (+)
          100
          (List.map
             (fun (code, _, _) -> code)
             command_map
           |> List.max
           |> Option.get_some)
      in
      Lwt.catch
        (fun () ->
         do_request
           code
           (fun buf () -> ()) ()
           (fun buf -> ())
           Lwt.return
         >>= fun () ->
         Lwt.fail_with "did not get an exception for unknown operation")
        (function
          | Error.Exn (Error.UnknownOperation, _) -> Lwt.return ()
          | exn -> Lwt.fail exn)

    method write_object_fs
        ~namespace ~object_name
        ~input_file
        ~allow_overwrite
        ?(checksum = None) () =
      self # request
        WriteObjectFs
        (namespace,
         object_name,
         input_file,
         allow_overwrite,
         checksum)

    method read_object_fs
      ~namespace ~object_name
      ~output_file
      ~consistent_read
      ~should_cache
      =
      self # request
        ReadObjectFs
        (namespace,
         object_name,
         output_file,
         consistent_read,
         should_cache)

    method read_object_slices ~namespace ~object_slices ~consistent_read =
      self # request ReadObjectsSlices (namespace, object_slices, consistent_read)

    method delete_object ~namespace ~object_name ~may_not_exist =
      self # request DeleteObject (namespace, object_name, may_not_exist)

    method apply_sequence ~namespace ~asserts ~updates ~write_barrier =
      self # request ApplySequence (namespace, write_barrier, asserts, updates)

    method multi_exists ~namespace ~object_names =
      self # request MultiExists (namespace, object_names)

    method read_objects : type o.
                               namespace : string ->
                               object_names : string list ->
                               consistent_read : bool ->
                               should_cache : bool ->
                               (int32 * (Nsm_model.Manifest.t * Bigstring_slice.t) option list -> o Lwt.t) ->
                               o Lwt.t
      =
      fun ~namespace ~object_names ~consistent_read ~should_cache f ->
      self # request'
           ReadObjects
           (namespace, object_names, consistent_read, should_cache)
           f

    method invalidate_cache ~namespace =
      self # request InvalidateCache namespace

    method statistics clear =
      self # request ProxyStatistics clear

    method get_version = self # request GetVersion ()

    method delete_namespace ~namespace =
      self # request DeleteNamespace namespace

    method list_object
             ~namespace ~first
             ~finc ~last ~max
             ~reverse
      = self # request ListObjects
             (namespace, RangeQueryArgs.({first; finc; last; max; reverse}))

    method create_namespace ~namespace ~preset_name =
      self # request CreateNamespace (namespace, preset_name)

    method list_namespaces ~first ~finc ~last
                          ~max ~reverse
      = self # request ListNamespaces
             RangeQueryArgs.{ first; finc; last; max; reverse; }

    method osd_view = self # request OsdView ()

    method get_client_config = self # request GetClientConfig ()

    method osd_info = self # request OsdInfo ()
  end

let _prologue fd magic version =
  let buf = Buffer.create 8 in
  Llio.int32_to buf magic;
  Llio.int32_to buf version;
  Net_fd.write_all' fd (Buffer.contents buf)

let make_client ip port transport =
  Networking2.connect_with
    ~tls_config:None
    ip port transport
  >>= fun (nfd, closer) ->
  Lwt.catch
    (fun () -> _prologue nfd Protocol.magic Protocol.version >>= fun () ->
               Lwt.return (new proxy_client nfd, closer))
    (fun exn ->
      closer () >>= fun () ->
      Lwt.fail exn)

let with_client ip port transport f =
  make_client ip port transport >>= fun (client, closer) ->
  Lwt.finalize
    (fun () -> f client)
    closer
