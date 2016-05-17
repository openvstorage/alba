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
open Lwt.Infix
open Proxy_protocol
open Protocol
open Range_query_args


class proxy_client fd =
  let read_response deserializer =
    let module Llio = Llio2.NetFdReader in
    Llio.int_from fd >>= fun size ->
    Llio.with_buffer_from
      fd size
      (fun res_buf ->
       match Llio2.ReadBuffer.int_from res_buf with
       | 0 ->
          Lwt.return (deserializer res_buf)
       | err ->
          let err_string = Llio2.ReadBuffer.string_from res_buf in
          Lwt_log.debug_f "Proxy client received error from server: %s" err_string
          >>= fun () -> Error.failwith (Error.int2err err))
  in
  let do_request code serialize_request request response_deserializer =
    let module Llio = Llio2.WriteBuffer in
    let buf =
      Llio.serialize_with_length
        (Llio.pair_to
           Llio.int_to
           serialize_request)
        (code,
         request)
    in

    Net_fd.write_all_lwt_bytes
      fd buf.Llio.buf 0 buf.Llio.pos
    >>= fun () ->

    read_response response_deserializer
  in
  object(self)
    method private request : type i o. (i, o) request -> i -> o Lwt.t =
      fun command req ->
      do_request
        (command_to_code (Wrap command))
        (deser_request_i command |> snd) req
        (Deser.from_buffer (deser_request_o command))

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
           (fun buf -> ()) >>= fun () ->
         Lwt.fail_with "did not get an exception for unknown operation")
        (function
          | Error.Exn Error.UnknownOperation -> Lwt.return ()
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

    method delete_object ~namespace ~object_name ~may_not_exist=
      self # request DeleteObject (namespace, object_name, may_not_exist)

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

    method list_namespace ~first ~finc ~last
                          ~max ~reverse
      = self # request ListNamespaces
             RangeQueryArgs.{ first; finc; last; max; reverse; }

    method osd_view = self # request OsdView ()

    method get_client_config = self # request GetClientConfig ()
  end

let _prologue fd magic version =
  let buf = Buffer.create 8 in
  Llio.int32_to buf magic;
  Llio.int32_to buf version;
  Net_fd.write_all' fd (Buffer.contents buf)

let with_client ip port transport f =
  Networking2.connect_with
    ~tls_config:None
    ip port transport
  >>= fun (nfd, closer) ->
  Lwt.finalize
    (fun () ->
     _prologue nfd Protocol.magic Protocol.version >>= fun () ->
     f (new proxy_client nfd))
    closer
