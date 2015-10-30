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
open Slice
open Asd_protocol
open Protocol

class client fd ic id =
  let read_response deserializer =
    Llio.input_string ic >>= fun res_s ->
    let res_buf = Llio.make_buffer res_s 0 in
    match Llio.int_from res_buf with
    | 0 ->
      Lwt.return (deserializer res_buf)
    | err ->
      let open Error in
      let err' = deserialize' err res_buf in
      Lwt_log.debug_f "Exception in asd_client %s: %s" id (show err') >>= fun () ->
      lwt_fail err'
  in
  object(self)
    method private query : type req res. (req, res) query -> req -> res Lwt.t =
      fun command req ->
        let descr = code_to_description (command_to_code (Wrap_query command)) in
        Lwt_log.debug_f
          "asd_client %s: %s"
          id descr >>= fun () ->
        with_timing_lwt
          (fun () ->
           let s =
             serialize_with_length
               (Llio.pair_to
                  Llio.int32_to
                  (query_request_serializer command))
               (command_to_code (Wrap_query command),
                req)
           in
           Lwt_extra2.write_all' fd s >>= fun () ->
           read_response (query_response_deserializer command)) >>= fun (t, r) ->
        Lwt_log.debug_f "asd_client %s: %s took %f" id descr t >>= fun () ->
        Lwt.return r

    method private update : type req res. (req, res) update -> req -> res Lwt.t =
      fun command req ->
        let descr = code_to_description (command_to_code (Wrap_update command)) in
        Lwt_log.debug_f
          "asd_client %s: %s"
          id descr >>= fun () ->
        with_timing_lwt
          (fun () ->
           let s =
             serialize_with_length
               (Llio.pair_to
                  Llio.int32_to
                  (update_request_serializer command))
               (command_to_code (Wrap_update command),
                req)
           in
           Lwt_extra2.write_all' fd s >>= fun () ->
           read_response (update_response_deserializer command)) >>= fun (t, r) ->
        Lwt_log.debug_f "asd_client %s: %s took %f" id descr t >>= fun () ->
        Lwt.return r

    val mutable supports_multiget2 = None
    method multi_get ~prio keys =
      let old_multiget () =
        self # query MultiGet (keys, prio)
      in
      match supports_multiget2 with
      | None ->
         (* try multiget2, if it succeeds the asd supports it *)
         Lwt.catch
           (fun () ->
            self # multi_get2 ~prio keys >>= fun res ->
            supports_multiget2 <- Some true;
            Lwt.return res)
           (function
             | Error.Exn Error.Unknown_operation ->
                supports_multiget2 <- Some false;
                old_multiget ()
             | exn ->
                Lwt.fail exn)
      | Some true ->
         self # multi_get2 ~prio keys
      | Some false ->
         old_multiget ()

    method multi_get2 ~prio keys =
      self # query MultiGet2 (keys, prio) >>= fun res ->
      Lwt_list.map_s
        (let open Value in
         function
          | None -> Lwt.return_none
          | Some (blob, cs) ->
             match blob with
             | Direct s -> Lwt.return (Some (s, cs))
             | Later size ->
                let target = Bytes.create size in

                let buffered = Lwt_io.buffered ic in
                (if size <= buffered
                 then
                   begin
                     Lwt_io.read_into ic target 0 size >>= fun read ->
                     assert (read = size);
                     Lwt.return ()
                   end
                 else
                   begin
                     (if buffered > 0
                      then Lwt_io.read_into ic target 0 buffered
                      else Lwt.return 0) >>= fun read ->
                     assert (read = buffered);
                     assert (0 = Lwt_io.buffered ic);

                     let remaining = size - read in
                     Lwt_extra2.read_all
                       fd target
                       read remaining
                     >>= fun read' ->
                     if read' = remaining
                     then Lwt.return ()
                     else begin
                         Lwt_log.debug_f
                           "read=%i, read'=%i, size=%i, buffered=%i, new buffered=%i"
                           read read' size
                           buffered (Lwt_io.buffered ic)
                         >>= fun () ->
                         Lwt.fail End_of_file
                     end
                   end) >>= fun () ->

                Lwt.return (Some (Slice.wrap_bytes target, cs)))
        res

    method multi_get_string ~prio keys =
      self # multi_get ~prio (List.map Slice.wrap_string keys) >>= fun res ->
      Lwt.return
        (List.map
           (Option.map (fun (slice, cs) -> Slice.get_string_unsafe slice, cs))
           res)

    method multi_exists ~prio keys = self # query MultiExists (keys, prio)

    method get ~prio key =
      self # multi_get ~prio [ key ] >>= fun res ->
      List.hd_exn res |>
      Lwt.return

    method get_string ~prio key =
      self # multi_get_string ~prio [ key ] >>= fun res ->
      List.hd_exn res |>
      Lwt.return

    method set ~prio key value assertable ?(cs = Checksum.Checksum.NoChecksum) () =
      let u = Update.set key value cs assertable in
      self # apply_sequence ~prio [] [u] >>= fun _ ->
      Lwt.return ()

    method set_string ~prio ?(cs = Checksum.Checksum.NoChecksum)
             key value assertable
      =
      let u = Update.set_string key value cs assertable in
      self # apply_sequence ~prio [] [u] >>= fun _ ->
      Lwt.return ()

    method delete ~prio key =
      self # apply_sequence ~prio [] [ Update.delete key ] >>= fun _ ->
      Lwt.return ()

    method delete_string ~prio key =
      self # apply_sequence ~prio [] [ Update.delete_string key ] >>= fun _ ->
      Lwt.return ()

    method range ~prio ~first ~finc ~last ~reverse ~max =
      self # query
        Range
        ({ first; finc; last; reverse; max }, prio)

    method range_all ~prio ?(max = -1) () =
      list_all_x
        ~first:(Slice.wrap_string "")
        Std.id
        (self # range ~prio ~last:None ~max ~reverse:false)

    method range_string ~prio ~first ~finc ~last ~reverse ~max =
      self # range
        ~prio
        ~first:(Slice.wrap_string first) ~finc
        ~last:(Option.map (fun (l, linc) -> Slice.wrap_string l, linc) last)
        ~max ~reverse >>= fun ((cnt, keys), has_more) ->
      Lwt.return ((cnt, List.map Slice.get_string_unsafe keys), has_more)

    method range_entries ~prio ~first ~finc ~last ~reverse ~max =
      self # query
        RangeEntries
        ({ first; finc; last; reverse; max; }, prio)

    method apply_sequence ~prio asserts updates =
      self # update Apply (asserts, updates, prio)

    method statistics clear =
      self # query Statistics clear

    method set_full full =
      self # update SetFull full

    method get_version () =
      self # query GetVersion ()

    method get_disk_usage () =
      self # query GetDiskUsage ()
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
         | _ -> Lwt.return asd_id'
       end
    | err -> Error.from_stream (Int32.to_int err) ic



let make_client buffer_pool ips port (lido:string option) =
  Networking2.first_connection ips port
  >>= fun (fd, closer) ->
  let buffer = Buffer_pool.get_buffer buffer_pool in
  let ic = Lwt_io.of_fd ~buffer ~mode:Lwt_io.input fd in
  let closer () =
    Buffer_pool.return_buffer buffer_pool buffer;
    closer ()
  in
  Lwt.catch
    (fun () ->
       let open Asd_protocol in
       let prologue_bytes = make_prologue _MAGIC _VERSION lido in
       Lwt_extra2.write_all' fd prologue_bytes >>= fun () ->
       _prologue_response ic lido >>= fun long_id ->
       let client = new client fd ic long_id in
       Lwt.return (client, closer)
    )
    (fun exn ->
     closer () >>= fun () ->
     Lwt.fail exn)

let with_client buffer_pool ips port (lido:string option) f =
  (* TODO: validation here? or elsewhere *)
  if ips = []
  then failwith "empty ips list for asd_client.with_client";

  make_client buffer_pool ips port lido >>= fun (client, closer) ->
  Lwt.finalize
    (fun () -> f client)
    closer

class asd_osd (asd_id : string) (asd : client) =
  object(self :# Osd.osd)
  method get_option prio (k:key) =
    asd # multi_get ~prio [k] >>= fun vcos ->
    let ho = List.hd_exn vcos in
    let r =
      match ho with
      | None -> None
      | Some (v,c) -> Some v
    in
    Lwt.return r

  method get_exn prio (k:key) =
    self # get_option prio k
    >>= function
    | None -> Lwt.fail (Failure (Printf.sprintf
                                   "Could not find key %s on asd %S"
                                   (Slice.get_string_unsafe k) asd_id))
    | Some v -> Lwt.return v

  method multi_get prio keys =
    asd # multi_get ~prio keys >>= fun vcos ->
    Lwt.return
      (List.map
         (Option.map fst)
         vcos)

  method multi_exists prio keys = asd # multi_exists ~prio keys

  method range prio = asd # range ~prio

  method range_entries prio = asd # range_entries ~prio

  method apply_sequence prio asserts (upds: Update.t list) =
    Lwt.catch
      (fun () ->
         asd # apply_sequence ~prio asserts upds >>= fun () ->
         Lwt.return Osd.Ok)
      (function
        | Error.Exn e ->
           Lwt.return (Osd.Exn e)
        | exn -> Lwt.fail exn)

  method set_full full = asd # set_full full

  method get_version = asd # get_version ()
  method get_long_id = asd_id
  method get_disk_usage = asd # get_disk_usage ()
end
