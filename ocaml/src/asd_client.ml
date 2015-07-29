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

open Prelude
open Lwt
open Slice
open Asd_protocol
open Protocol

class client (ic, oc) =
  let read_response deserializer =
    Llio.input_string ic >>= fun res_s ->
    let res_buf = Llio.make_buffer res_s 0 in
    match Llio.int_from res_buf with
    | 0 ->
      Lwt.return (deserializer res_buf)
    | err ->
      let open Error in
      let err' = deserialize' err res_buf in
      Lwt_log.debug_f "Exception in asd_client: %s" (show err') >>= fun () ->
      lwt_fail err'
  in
  object(self)
    method private query : type req res. (req, res) query -> req -> res Lwt.t =
      fun command req ->
        let descr = code_to_description (command_to_code (Wrap_query command)) in
        Lwt_log.debug_f
          "asd_client: %s"
          descr >>= fun () ->
        Alba_statistics.Statistics.with_timing_lwt
          (fun () ->
             let buf = Buffer.create 20 in
             Llio.int_to buf (command_to_code (Wrap_query command));
             query_request_serializer command buf req;
             Lwt_extra2.llio_output_and_flush oc (Buffer.contents buf) >>= fun () ->
             read_response (query_response_deserializer command)) >>= fun (t, r) ->
        Lwt_log.debug_f "asd_client: %s took %f" descr t >>= fun () ->
        Lwt.return r

    method private update : type req res. (req, res) update -> req -> res Lwt.t =
      fun command req ->
        let descr = code_to_description (command_to_code (Wrap_update command)) in
        Lwt_log.debug_f
          "asd_client: %s"
          descr >>= fun () ->
        Alba_statistics.Statistics.with_timing_lwt
          (fun () ->
             let buf = Buffer.create 20 in
             Llio.int_to buf (command_to_code (Wrap_update command));
             update_request_serializer command buf req;
             Lwt_extra2.llio_output_and_flush oc (Buffer.contents buf) >>= fun () ->
             read_response (update_response_deserializer command)) >>= fun (t, r) ->
        Lwt_log.debug_f "asd_client: %s took %f" descr t >>= fun () ->
        Lwt.return r

    method multi_get keys =
      self # query MultiGet keys

    method multi_get_string keys =
      self # multi_get (List.map Slice.wrap_string keys) >>= fun res ->
      Lwt.return
        (List.map
           (Option.map (fun (slice, cs) -> Slice.get_string_unsafe slice, cs))
           res)

    method get key =
      self # multi_get [ key ] >>= fun res ->
      List.hd_exn res |>
      Lwt.return

    method get_string key =
      self # multi_get_string [ key ] >>= fun res ->
      List.hd_exn res |>
      Lwt.return

    method set key value assertable ?(cs = Checksum.Checksum.NoChecksum) () =
      let u = Update.set key value cs assertable in
      self # update Apply ([], [u]) >>= fun _ ->
      Lwt.return ()

    method set_string ?(cs = Checksum.Checksum.NoChecksum)
             key value assertable
      =
      let u = Update.set_string key value cs assertable in
      self # update Apply ([], [u]) >>= fun _ ->
      Lwt.return ()

    method delete key =
      self # update Apply ([], [ Update.delete key ]) >>= fun _ ->
      Lwt.return ()

    method delete_string key =
      self # update Apply ([], [ Update.delete_string key ]) >>= fun _ ->
      Lwt.return ()

    method range ~first ~finc ~last ~reverse ~max =
      self # query
        Range
        { first; finc; last; reverse; max }

    method range_all ?(max = -1) () =
      list_all_x
        ~first:(Slice.wrap_string "")
        Std.id
        (self # range ~last:None ~max ~reverse:false)

    method range_string ~first ~finc ~last ~reverse ~max =
      self # range
        ~first:(Slice.wrap_string first) ~finc
        ~last:(Option.map (fun (l, linc) -> Slice.wrap_string l, linc) last)
        ~max ~reverse >>= fun ((cnt, keys), has_more) ->
      Lwt.return ((cnt, List.map Slice.get_string_unsafe keys), has_more)

    method range_entries ~first ~finc ~last ~reverse ~max =
      self # query
        RangeEntries
        { first; finc; last; reverse; max; }

    method apply_sequence asserts updates =
      self # update Apply (asserts, updates)

    method statistics clear =
      self # query Statistics clear

    method set_full full =
      self # update SetFull full
    method get_version () =
      self # query GetVersion ()
  end

exception BadLongId of string * string

let make_prologue magic version lido =
  let buf = Buffer.create 16 in
  Buffer.add_string buf magic;
  Llio.int32_to     buf version;
  Llio.string_option_to buf lido;
  Buffer.contents buf

let _prologue_response ic lido =
  Llio.input_int32 ic >>=
    function
    | 0l ->
       begin
         Llio.input_string ic >>= fun asd_id' ->
         match lido with
         | Some asd_id when asd_id <> asd_id' ->
            Lwt.fail (BadLongId (asd_id, asd_id'))
         | _ -> Lwt.return ()
       end
    | err -> Error.from_stream (Int32.to_int err) ic



let make_client ips port (lido:string option) =
  Networking2.first_connection ~buffer_size:(768*1024) ips port >>= fun conn ->
  let closer = Networking2.closer conn in
  Lwt.catch
    (fun () ->
       let ic,oc = conn in
       let open Asd_protocol in
       let prologue_bytes = make_prologue _MAGIC _VERSION lido in
       Lwt_io.write oc prologue_bytes >>= fun () ->
       _prologue_response ic lido >>= fun () ->
       let client = new client conn in
       Lwt.return (client, closer)
    )
    (fun exn ->
     closer () >>= fun () ->
     Lwt.fail exn)

let with_client ips port (lido:string option) f =
  (* TODO: validation here? or elsewhere *)
  if ips = []
  then failwith "empty ips list for asd_client.with_client";

  make_client ips port lido >>= fun (client, closer) ->
  Lwt.finalize
    (fun () -> f client)
    closer

class asd_osd (asd_id : string) (asd : client) =
  object(self :# Osd.osd)
  method get_option (k:key) =
    asd # multi_get [k] >>= fun vcos ->
    let ho = List.hd_exn vcos in
    let r =
      match ho with
      | None -> None
      | Some (v,c) -> Some v
    in
    Lwt.return r

  method get_exn (k:key) =
    self # get_option k
    >>= function
    | None -> Lwt.fail (Failure (Printf.sprintf
                                   "Could not find key %s on asd %S"
                                   (Slice.get_string_unsafe k) asd_id))
    | Some v -> Lwt.return v

  method multi_get keys =
    asd # multi_get keys >>= fun vcos ->
    Lwt.return
      (List.map
         (Option.map fst)
         vcos)

  method range = asd # range

  method range_entries = asd # range_entries

  method apply_sequence asserts (upds: Update.t list) =
    Lwt_log.debug "asd_client: apply_sequence" >>= fun () ->
    Lwt.catch
      (fun () ->
         asd # apply_sequence asserts upds >>= fun () ->
         Lwt.return Osd.Ok)
      (function
        | Error.Exn e ->
           Lwt.return (Osd.Exn e)
        | exn -> Lwt.fail exn)

  method set_full full = asd # set_full full

  method get_version = asd # get_version ()
  method get_long_id = asd_id
end
