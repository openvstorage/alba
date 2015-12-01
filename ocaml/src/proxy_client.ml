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
open Lwt
open Proxy_protocol
open Protocol


class proxy_client (ic, oc) =
  let read_response deserializer =
    Llio.input_string ic >>= fun res_s ->
    let res_buf = Llio.make_buffer res_s 0 in
    match Llio.int_from res_buf with
    | 0 ->
      Lwt.return (deserializer res_buf)
    | err ->
      let err_string = Llio.string_from res_buf in
      Lwt_log.debug_f "Proxy client received error from server: %s" err_string
      >>= fun () -> Error.failwith (Error.int2err err)
  in
  let do_request code request_serializer response_deserializer =
    let buf = Buffer.create 20 in
    Llio.int_to buf code;
    request_serializer buf;
    Lwt_extra2.llio_output_and_flush oc (Buffer.contents buf) >>= fun () ->
    read_response response_deserializer
  in
  object(self)
    method private request : type i o. (i, o) request -> i -> o Lwt.t =
      fun command req ->
      do_request
        (command_to_code (Wrap command))
        (fun buf -> Deser.to_buffer (deser_request_i command) buf req)
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
           (fun buf -> ())
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

let _prologue oc magic version =
  Llio.output_int32 oc magic >>= fun () ->
  Llio.output_int32 oc version

let with_client ip port f =
  Lwt_io.with_connection
    (Networking2.make_address ip port)
    (fun conn ->
     _prologue (snd conn) Protocol.magic Protocol.version >>= fun () ->
     f (new proxy_client conn))
