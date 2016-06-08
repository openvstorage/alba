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

open Lwt.Infix
open Prelude
open Rocks_store
open Asd_protocol
open Slice
open Checksum
open Asd_statistics
open Asd_io_scheduler

open Range_query_args
open Blob_access
open Blob_access_factory

let blob_threshold = 16 * 1024

let stats_tag_to_string code =
  try Asd_protocol.Protocol.code_to_description code
  with
  | Not_found ->
     begin
       try Blob_access.code_to_description code
       with
       | Not_found -> Printf.sprintf "unknown operation %li" code
     end

module KVS = Key_value_store

module Keys = struct
  let node_id = "*node_id"
  let asd_id = "*asd_id"
  let disk_usage = "*disk_usage"

  let public_prefix_byte = '\000'
  let public_key_next_prefix = Some ("\001", false)

  let file_to_key_mapping_prefix = '\001'
  let file_to_key_mapping_next_prefix = "\002"

  let to_be_deleted_prefix = '\002'
  let to_be_deleted_first = "\002"
  let to_be_deleted_last = Some ("\003", false)

  let check_garbage_from = "\003"

  let file_to_key_mapping fnr =
    serialize
      (Llio.pair_to
         Llio.char_to
         Llio.int64_be_to)
      (file_to_key_mapping_prefix,
       fnr)

  let file_to_key_extract_fnr key =
    let c, fnr =
      deserialize
        (Llio.pair_from
           Llio.char_from
           Llio.int64_be_from)
        key in
    assert (c = file_to_key_mapping_prefix);
    fnr

  let to_be_deleted fnr =
    serialize
      (Llio.pair_to
         Llio.char_to
         Llio.int64_to)
      (to_be_deleted_prefix,
       fnr)

  let key_with_public_prefix s = Slice.add_prefix_byte_as_bytes s public_prefix_byte
  let string_chop_prefix v = Slice.make v 1 (String.length v - 1)

end

module Value = struct
  type blob =
    | Direct of Slice.t
    | OnFs of Int64.t * int      (* file number * size *)
  [@@deriving show]

  type t = Checksum.t * blob
  [@@deriving show]

  let blob_to_buffer buf = function
    | Direct value ->
      Llio.int8_to buf 1;
      Slice.to_buffer buf value
    | OnFs (fnr, size) ->
      Llio.int8_to buf 2;
      Llio.int64_to buf fnr;
      Llio.int_to buf size

  let blob_from_buffer buf =
    match Llio.int8_from buf with
    | 1 ->
      let value = Slice.from_buffer buf in
      Direct value
    | 2 ->
      let fnr = Llio.int64_from buf in
      let size = Llio.int_from buf in
      OnFs (fnr, size)
    | k -> Prelude.raise_bad_tag "Asd_server.Value.blob" k

  let to_buffer buf (cs, blob) =
    Checksum.output buf cs;
    blob_to_buffer buf blob

  let from_buffer buf =
    let cs = Checksum.input buf in
    let blob = blob_from_buffer buf in
    (cs, blob)

  let get_blob_from_value dir_info t = match snd t with
    | Direct value ->
      Lwt.return value
    | OnFs (fnr, size) ->
      dir_info # get_blob fnr size >>=
      compose Lwt.return Slice.wrap_string

  let get_cs = fst

  let get_size = function
    | Direct s -> Slice.length s
    | OnFs (_, size) -> size
end

let ro = Rocks.ReadOptions.create_no_gc ()
let wo_sync =
  let open Rocks in
  let r = WriteOptions.create_no_gc () in
  WriteOptions.set_sync r true;
  r
let wo_no_sync =
  let open Rocks in
  let r = WriteOptions.create_no_gc () in
  WriteOptions.set_sync r false;
  r
let wo_no_wal =
  let open Rocks in
  let r = WriteOptions.create_no_gc () in
  WriteOptions.set_sync r false;
  WriteOptions.set_disable_WAL r true;
  r

let get_value_option kv key =
  let open Rocks in
  let vo_raw =
    Slice.with_prefix_byte_unsafe
      key
      Keys.public_prefix_byte
      (fun key' ->
         let open Slice in
         RocksDb.get_slice
           kv ro
           key'.buf key'.offset key'.length)
  in
  Option.map (fun v -> deserialize Value.from_buffer v) vo_raw

let key_exists kv key = (get_value_option kv key) <> None

let execute_query : type req res.
                         Rocks_key_value_store.t ->
                         (Llio2.WriteBuffer.t * (Net_fd.t ->
                                                 unit Lwt.t)) Asd_io_scheduler.t ->
                         Blob_access.directory_info ->
                         AsdMgmt.t ->
                         AsdStatistics.t ->
                         (req, res) Protocol.query ->
                         req ->
                         (Llio2.WriteBuffer.t * (Net_fd.t ->
                                                 unit Lwt.t)) Lwt.t
  = fun kv io_sched dir_info mgmt stats q ->

    let open Protocol in
    let module Llio = Llio2.WriteBuffer in
    let serialize_with_length res =
      Llio.serialize_with_length
        (Llio.pair_to
           Llio.int_to
           (Protocol.query_response_serializer q))
        (0, res)
    in
    let return'' ~cost (res, write_extra) =
      Lwt.return ((serialize_with_length res, write_extra), cost) in
    let return ~cost res = return'' ~cost (res, fun _ -> Lwt.return_unit) in
    let return' res = Lwt.return (serialize_with_length res, fun _ -> Lwt.return_unit) in
    match q with
    | Range -> fun ({ RangeQueryArgs.first; finc; last; reverse; max; }, prio) ->
      perform_read
        io_sched
        prio
        (fun () ->
         let max = cap_max ~max () in
         let (count, keys), have_more =
           Rocks_key_value_store.range
             kv
             ~first:(Keys.key_with_public_prefix first) ~finc
             ~last:(match last with
                    | None -> Keys.public_key_next_prefix
                    | Some (last, linc) -> Some (Keys.key_with_public_prefix last, linc))
             ~reverse
             ~max
         in
         return
           ~cost:(max * 200)
           ((count, List.map Keys.string_chop_prefix keys),
            have_more))
    | RangeEntries -> fun ({ RangeQueryArgs.first; finc; last; reverse; max; }, prio) ->
      perform_read
        io_sched
        prio
        (fun () ->
         let (cnt, is), has_more =
           Rocks_key_value_store.map_range
             kv
             ~first:(Keys.key_with_public_prefix first) ~finc
             ~last:(match last with
                    | None -> Keys.public_key_next_prefix
                    | Some (last, linc) -> Some (Keys.key_with_public_prefix last, linc))
             ~reverse
             ~max:(cap_max ~max ())
             (fun cur key ->
              Keys.string_chop_prefix key,
              deserialize Value.from_buffer (Rocks_key_value_store.cur_get_value cur)
             )
         in

         Lwt_list.map_p
           (fun (k, vt) ->
            Value.get_blob_from_value dir_info vt >>= fun blob ->
            Lwt.return (k, Slice.to_bigstring blob |> Bigstring_slice.wrap_bigstring,
                        fst vt))
           is >>= fun blobs ->

         return
           ~cost:(List.fold_left
                    (fun acc (_, blob, _) -> acc + 200 + Bigstring_slice.length blob)
                    0
                    blobs)
           ((cnt, blobs),
            has_more))
    | MultiGet -> fun (keys, prio) ->
      perform_read
        io_sched
        prio
        (fun () ->
         Lwt_log.debug_f "MultiGet for %s" ([%show: Slice.t list] keys) >>= fun () ->
         (* first determine atomically which are the relevant Value.t's *)
         List.map
           (fun k -> get_value_option kv k)
           keys |>
           (* then get all the blobs from the file system concurrently *)
           Lwt_list.map_p
             (function
               | None -> Lwt.return None
               | Some v ->
                  Value.get_blob_from_value dir_info v >>= fun b ->
                  Lwt.return (Some (Slice.to_bigstring b |> Bigstring_slice.wrap_bigstring,
                                    Value.get_cs v))) >>= fun res ->
         return
           ~cost:(List.fold_left
                    (fun acc ->
                     function
                     | None           -> acc + 200
                     | Some (blob, _) -> acc + 200 + Bigstring_slice.length blob)
                    0
                    res)
           res)
    | MultiGet2 -> fun (keys, prio) ->
      if not (dir_info # config).write_blobs
      then
        (* the trick with /dev/zero doesn't work with
         * sendfile, so let's avoid sendfile by pretending this
         * asd doesn't support multiget2 *)
        Error.failwith Error.Unknown_operation
      else
        begin
      perform_read
        io_sched
        prio
        (fun () ->
         Lwt_log.debug_f "MultiGet2 for %s" ([%show: Slice.t list] keys) >>= fun () ->
         (* first determine atomically which are the relevant Value.t's *)
         let write_laters = ref [] in
         let res =
           List.map
             (fun k ->
              get_value_option kv k
              |> Option.map
                   (fun (cs, blob) ->
                    let b = match blob with
                      | Value.Direct s ->
                         Asd_protocol.Value.Direct (Slice.to_bigstring s
                                                    |> Bigstring_slice.wrap_bigstring)
                      | Value.OnFs (fnr, size) ->
                         write_laters := (fnr, size) :: !write_laters;
                         Asd_protocol.Value.Later size in
                    b, cs))
             keys
         in

         return''
           ~cost:(List.fold_left
                    (fun acc ->
                     function
                     | None           -> acc + 200
                     | Some (Asd_protocol.Value.Direct blob, _) -> acc + 200 + Bigstring_slice.length blob
                     | Some (Asd_protocol.Value.Later size, _) -> acc + 200 + size)
                    0
                    res)
           (res,
            fun nfd ->
            Lwt_list.iter_s
              (fun (fnr, size) -> dir_info # send_blob_data_to fnr size [0,size] nfd)
              (List.rev !write_laters)))
        end
    | MultiExists -> fun (keys, prio) ->
                     perform_read
                       io_sched prio
                       (fun () ->
                        let res = List.map (fun k -> key_exists kv k) keys in
                        return
                          ~cost:(200 * List.length keys)
                          res)
    | PartialGet ->
       fun (key, slices, prio) ->
       begin
         Lwt_log.debug_f "PartialGet for %s" (Slice.show key) >>= fun () ->
         let took, vo = Prelude.with_timing (fun () -> get_value_option kv key) in
         let () = AsdStatistics.new_delta stats _ROCKS_LOOKUP took in
         match vo with
         | None -> return' false
         | Some (_cs, blob) ->
            let cost =
              List.fold_left
                (fun acc (_, length) ->
                 acc + 200 + length)
                0
                slices
            in
            perform_read
              io_sched
              prio
              (fun () ->
               let write_later nfd =
                 match blob with
                 | Value.Direct s ->
                    (* TODO could optimize the number of syscalls using writev *)
                    Lwt_list.iter_s
                      (fun (offset, len) -> Net_fd.write_all
                                              nfd
                                              s.Slice.buf (s.Slice.offset + offset) len)
                      slices
                 | Value.OnFs (fnr, size) ->
                    let cfg = dir_info # config in
                    if cfg.write_blobs
                    then
                      begin
                        dir_info # send_blob_data_to fnr size slices nfd
                      end
                    else
                      Lwt_list.iter_s
                        (fun (_, len) ->
                         Net_fd.write_all nfd (Bytes.create len) 0 len)
                        slices
               in
               return'' ~cost (true, write_later))
       end
    | Statistics -> fun clear ->
                    Asd_statistics.AsdStatistics.snapshot stats clear |> return'
    | GetVersion -> fun () ->
                    return' Alba_version.summary
    | GetDiskUsage ->
       fun () ->
       let open AsdMgmt in
       return' (mgmt.latest_disk_usage, !(mgmt.capacity))

exception ConcurrentModification

let cleanup_files_to_delete ignore_unlink_error _ kv dir_info fnrs =
  if fnrs = []
  then Lwt.return ()
  else begin
    dir_info # delete_blobs fnrs ~ignore_unlink_error
    >>= fun () ->

    List.iter
      (fun fnr ->
         (* file is deleted, now we can notify this
            back to the index in rocksdb (this needs no syncing / WAL) *)
         let key = Keys.to_be_deleted fnr in
         Rocks.RocksDb.delete
           kv
           wo_no_wal
           key)
      fnrs;

    Lwt.return ()
  end

let maybe_delete_file kv dir_info fnr =
  match (Rocks.RocksDb.get
           kv ro
           (Keys.file_to_key_mapping fnr)) with
  | Some _ ->
    (* file is used by a key value pair, don't delete it *)
    Lwt.return ()
  | None ->
     dir_info # delete_blobs [fnr] ~ignore_unlink_error:false >>= fun () ->
     Rocks.RocksDb.delete
       kv
       wo_no_wal
       (Keys.to_be_deleted fnr);
     Lwt.return_unit


let execute_update : type req res.
  Rocks_key_value_store.t ->
  release_fnr : (int64 -> unit) ->
  (Llio2.WriteBuffer.t * (Net_fd.t ->
                          unit Lwt.t)) Asd_io_scheduler.t ->
  directory_info ->
  mgmt: AsdMgmt.t ->
  get_next_fnr : (unit -> int64) ->
  (req, res) Protocol.update ->
  req ->
  res Lwt.t
  = fun kv ~release_fnr io_sched dir_info
        ~mgmt ~get_next_fnr ->
    let open Protocol in
    function
    | Apply -> fun (asserts, upds, prio) ->
      begin
        Lwt_log.debug_f
          "Apply with asserts = %s & upds = %s"
          ([%show: Assert.t list] asserts)
          ([%show: Update.t list] upds)
        >>= fun () ->
        let () =
          if not (AsdMgmt.updates_allowed mgmt upds )
          then Error.failwith Error.Full
        in
        let transform_asserts () =
          (* translate asserts that may take some time into 'immediate' asserts *)
          let open Assert in
          let none_asserts, some_asserts =
            List.partition
              is_none_assert
              asserts in

          let none_asserts' =
            List.map
              (fun a -> `AssertNone (key_of a))
              none_asserts
          in

          let some_asserts_with_values =
            List.map
              (function Value (key, expected) ->
                        key,
                        Option.get_some expected,
                        get_value_option kv key)
              some_asserts
          in

          Lwt_list.map_p
            (fun (key, expected, value_o) ->
               match value_o with
               | None ->
                 Lwt_log.warning_f
                   "Assertion failed, expected some but got None instead" >>= fun () ->
                 Error.(failwith (Assert_failed (Slice.get_string_unsafe key)))
               | Some value ->
                 begin match snd value with
                   | Value.Direct _ ->
                     (* must be checked just before applying the transaction *)
                     Lwt.return (`AssertSome (key, expected, value))
                   | Value.OnFs (loc, size) ->
                     (* check the blob now, and just before applying the transaction
                        check whether the key is still associated with the same blob
                     *)
                     Value.get_blob_from_value dir_info value >>= fun blob ->
                     (* TODO make this comparison less costly *)
                     if (Slice.compare'
                           blob
                           (expected
                            |> Asd_protocol.Blob.get_string_unsafe
                            |> Slice.wrap_string)) <> Compare.EQ
                     then begin
                       Lwt_log.warning_f
                         "Assertion failed, expected some blob but got another" >>= fun () ->
                       Error.(failwith (Assert_failed (Slice.get_string_unsafe key)))
                     end else
                       (* could also be ok for other values ... *)
                       Lwt.return (`AssertSome (key, expected, value))
                 end)
            some_asserts_with_values >>= fun some_asserts' ->
          Lwt.return (none_asserts', some_asserts')
        in

        let with_immediate_updates f =

          let files = ref [] in
          let allow_getting_file = ref true in
          let get_file_path () =
            if !allow_getting_file
            then begin
                let fnr = get_next_fnr () in
                let file_path = dir_info # _get_file_path fnr in
                let sleep, awake = Lwt.wait () in
                files := (fnr, file_path, sleep) :: !files;
                fnr, file_path, awake
              end else
              failwith "can't allow taking a next file name as something is already failing"
          in

          let immediate_upds_promise =
            Lwt_list.map_p
              (function
                | Update.Set (key, Some (v, c, _)) ->
                   let blob_length = Asd_protocol.Blob.length v in
                   (if blob_length < blob_threshold
                    then begin
                        Lwt.return (Value.Direct (Asd_protocol.Blob.get_slice_unsafe v))
                      end else begin
                        let fnr, file_path, fnr_release = get_file_path () in
                        Lwt.finalize
                          (fun () ->
                           perform_write
                             io_sched
                             prio
                             [ fnr, v ])
                          (fun () ->
                           Lwt.wakeup fnr_release ();
                           Lwt.return ()) >>= fun () ->
                        Lwt.return (Value.OnFs (fnr, blob_length))
                      end) >>= fun value ->
                   let value' =
                     serialize
                       Value.to_buffer
                       (c, value) in
                   Lwt.return (key, `Set (value, value'))
                | Update.Set (key, None) ->
                   Lwt.return(key, `Delete))
              upds
          in
          Lwt.catch
            (fun () ->
             immediate_upds_promise >>= fun immediate_upds ->
             f immediate_upds >>= fun () ->

             (* the files are now definitely commited,
                release file numbers so that collect_garbage_from
                can be bumped *)
             allow_getting_file := false;
             List.iter
               (fun (fnr, _, _) -> release_fnr fnr)
               !files;
             Lwt.return ())
            (function
              | (Error.Exn (Error.Assert_failed _)) as exn ->

                 (* the files created here should be removed
                 as the sequence could not be applied *)
                 allow_getting_file := false;
                 Lwt.ignore_result begin
                     Lwt_list.iter_p
                       (fun (fnr, path, wait) ->
                        wait >>= fun () ->
                        Lwt_unix.unlink path >>= fun () ->
                        Lwt_extra2.fsync_dir_of_file path >>= fun () ->
                        release_fnr fnr;
                        Lwt.return ())
                       !files
                   end;
                 Lwt.fail exn
              | exn ->
                 (* an unknown exception occurred, so we're not sure
                 if the files we created are being used or not,
                 so for each file we call 'maybe_delete_file'
                 which does a check first *)
                 allow_getting_file := false;
                 Lwt.ignore_result begin
                     Lwt_list.iter_p
                       (fun (fnr, path, wait) ->
                        wait >>= fun () ->
                        maybe_delete_file kv dir_info fnr >>= fun () ->
                        release_fnr fnr;
                        Lwt.return ())
                       !files
                   end;
                 Lwt.fail exn)
        in

        let try_apply_immediate none_asserts some_asserts immediate_updates =

          List.iter
            (function
              | `AssertNone key ->
                 begin match get_value_option kv key with
                       | None -> ()
                       | Some _ ->
                          Lwt_log.ign_warning_f
                            "Assertion failed, key=%S expected none but got some"
                            ([%show : Slice.t] key);
                          Error.(failwith (Assert_failed (Slice.get_string_unsafe key)))
                 end)
            none_asserts;

          List.iter
            (function
              | `AssertSome (key, expected, expected_value) ->
                 begin match get_value_option kv key with
                       | None ->
                          Lwt_log.ign_warning_f
                            "Assertion failed, key=%S expected some but got none"
                            ([%show : Slice.t] key);
                          Error.(failwith (Assert_failed (Slice.get_string_unsafe key)))
                       | Some value ->
                          if value <> expected_value
                          then raise ConcurrentModification
                 end)
            some_asserts;

          (* if we get here none of the assertions failed
           so it's time to apply all the updates *)
          let open Rocks in
          WriteBatch.with_t
            (fun wb ->
             let files_to_delete, size_delta =
               List.fold_left
                 (fun (files_to_delete, size_delta) (key, action) ->
                  let files_to_delete, size_delta =
                    match get_value_option kv key with
                    | None -> files_to_delete, size_delta
                    | Some (_, Value.Direct v) ->
                       files_to_delete, size_delta - (Slice.length v)
                    | Some (_, Value.OnFs (fnr, file_size)) ->
                       (* there's currently a file associated with this key
                           that file should eventually be deleted... *)
                       WriteBatch.put
                         wb
                         (Keys.to_be_deleted fnr)
                         "";

                       (* remove file to key mapping *)
                       WriteBatch.delete
                         wb
                         (Keys.file_to_key_mapping fnr);

                       fnr :: files_to_delete,
                       size_delta - file_size
                  in
                  let size_delta =
                    size_delta +
                    match action with
                    | `Set (value, value') ->
                       let size =
                         let open Value in
                         match value with
                         | Direct v -> Slice.length v
                         | OnFs (fnr, size) ->
                            let () =
                              let file_map_key = Keys.file_to_key_mapping fnr in
                              let open Slice in
                              WriteBatch.put_slice
                                wb
                                file_map_key 0 (String.length file_map_key)
                                key.buf key.offset key.length
                            in
                            size
                       in
                       let open Slice in
                       with_prefix_byte_unsafe
                         key
                         Keys.public_prefix_byte
                         (fun key' ->
                          WriteBatch.put_slice
                            wb
                            key'.buf key'.offset key'.length
                            value' 0 (String.length value'));
                       size
                    | `Delete ->
                       let open Slice in
                       with_prefix_byte_unsafe
                         key
                         Keys.public_prefix_byte
                         (fun key' ->
                          WriteBatch.delete_slice
                            wb
                            key'.buf key'.offset key'.length);
                       0
                  in
                  files_to_delete, size_delta)
                 ([], 0)
                 immediate_updates in

             let () =
               let open Asd_protocol.AsdMgmt in
               mgmt.latest_disk_usage <- Int64.add
                                           mgmt.latest_disk_usage
                                           (Int64.of_int size_delta);
               WriteBatch.put
                 wb
                 Keys.disk_usage
                 (serialize Llio.int64_to mgmt.latest_disk_usage);
             in

             RocksDb.write kv wo_no_sync wb;

             files_to_delete)
        in

        with_immediate_updates
          (fun immediate_updates ->
           let rec inner = function
             | 5 -> Lwt.fail ConcurrentModification
             | attempt ->
                Lwt.catch
                  (fun () ->
                   transform_asserts () >>= fun (none_asserts, some_asserts) ->
                   let files_to_be_deleted =
                     try_apply_immediate
                       none_asserts some_asserts
                       immediate_updates in

                   sync_rocksdb io_sched >>= fun () ->

                   Lwt.ignore_result
                     (cleanup_files_to_delete false io_sched kv dir_info files_to_be_deleted);
                   Lwt.return `Succeeded)
                  (function
                    | Lwt.Canceled -> Lwt.fail Lwt.Canceled
                    | ConcurrentModification -> Lwt.return `Retry
                    | exn -> Lwt.fail exn) >>= function
                | `Succeeded -> Lwt.return ()
                | `Retry ->
                   Lwt_log.debug_f "Asd retrying apply due to concurrent modification" >>= fun () ->
                   inner (attempt + 1)
           in
           inner 0)
      end
    | SetFull -> fun full ->
      Lwt_log.warning_f "SetFull %b" full >>= fun () ->
      AsdMgmt.set_full mgmt full;
      Lwt.return ()

let check_node_id kv external_id =
  let internal_ido = Rocks_key_value_store.get kv Keys.node_id  in
  match internal_ido with
  | None ->
    (* first startup of the asd, store the node id *)
     Rocks_key_value_store.set kv Keys.node_id external_id
  | Some internal_id ->
    if internal_id <> external_id
    then failwith (Printf.sprintf "node id mismatch: %s <> %s" external_id internal_id)


let check_asd_id kv asd_id =
  match asd_id, Rocks_key_value_store.get kv Keys.asd_id with
  | None, Some asd_id ->
    asd_id
  | None, None ->
    let uuid = Uuidm.v4_gen (Random.State.make_self_init ()) () in
    let asd_id = Uuidm.to_string uuid in
    Rocks_key_value_store.set kv Keys.asd_id asd_id;
    asd_id
  | Some asd_id, None ->
    Rocks_key_value_store.set kv Keys.asd_id asd_id;
    asd_id
  | Some asd_id, Some asd_id' ->
    if asd_id = asd_id'
    then asd_id
    else failwith (Printf.sprintf "asd id mismatch: %s <> %s" asd_id asd_id')


let done_writing (nfd:Net_fd.t) = Lwt.return_unit

let asd_protocol
      ?cancel
      kv ~release_fnr ~slow io_sched
      dir_info stats ~mgmt
      ~get_next_fnr asd_id
      nfd
  =
  (* Lwt_log.debug "Waiting for request" >>= fun () -> *)
  let handle_request (buf : Llio2.ReadBuffer.t) code =
    (*
     Lwt_log.debug_f "Got request %s" (Protocol.code_to_description code)
  >>= fun () ->
     *)

    let module Llio = Llio2.WriteBuffer in
    let return_result
          ?(write_extra=done_writing)
          serializer res =
      let res_s =
        Llio.serialize_with_length
          (Llio.pair_to
             Llio.int_to
             serializer)
          (0, res)
      in
      Lwt.return (res_s, write_extra)
    in
    let return_error error =
      let res =
        Llio.serialize_with_length
          Protocol.Error.serialize
          error
      in
      Lwt.return (res, done_writing)
    in
    Lwt.catch
      (fun () ->
       let command = Protocol.code_to_command code in
       begin match command with
             | Protocol.Wrap_query q ->
                let req = Protocol.query_request_deserializer q buf in
                execute_query kv io_sched dir_info mgmt stats q req
             | Protocol.Wrap_update u ->
                let req = Protocol.update_request_deserializer u buf in
                execute_update
                  kv
                  ~release_fnr
                  io_sched
                  dir_info
                  ~mgmt
                  ~get_next_fnr
                  u req >>= fun res ->
                return_result (Protocol.update_response_serializer u) res
       end)
      (function
        | End_of_file as e ->
           Lwt.fail e
        | Protocol.Error.Exn err ->
           Lwt_log.info_f "returning error %s" (Protocol.Error.show err) >>= fun () ->
           return_error err
        | exn ->
           Lwt_log.info_f ~exn "error during client request: %s"
                          (Printexc.to_string exn)
           >>= fun () ->
           return_error (Protocol.Error.Unknown_error (1, "Unknown error occured"))
      ) >>= fun (res, write_extra) ->
    let () = Net_fd.cork nfd in
    Net_fd.write_all_lwt_bytes nfd res.Llio.buf 0 res.Llio.pos >>= fun () ->
    write_extra nfd >>= fun () ->
    let () = Net_fd.uncork nfd in
    Lwt.return_unit
  in
  let rec inner () =
    (match cancel with
     | None -> Llio2.NetFdReader.int_from nfd
     | Some cancel ->
        Lwt.pick
          [ (Lwt_condition.wait cancel >>= fun () ->
             Lwt.fail Lwt.Canceled);
            Llio2.NetFdReader.int_from nfd; ])
    >>= fun len ->
    Llio2.NetFdReader.with_buffer_from
      nfd len
      (fun buf ->
       let code = Llio2.ReadBuffer.int32_from buf in
       with_timing_lwt
         (fun () -> handle_request buf code >>= fun () ->
                    Lwt.return code))
    >>= fun (delta, code) ->
    Statistics_collection.Generic.new_delta stats code delta;

    (if slow
     then
       begin
         let delay = Random.float 3. in
         Lwt_log.info_f "Sleeping an additional %f" delay >>= fun () ->
         Lwt_unix.sleep delay
       end
     else
       Lwt.return_unit) >>= fun () ->

    (if delta > 0.5
     then Lwt_log.info_f
     else Lwt_log.debug_f)
      "Request %s took %f" (Protocol.code_to_description_nothrow code) delta >>= fun () ->
    inner ()
  in
  Llio2.NetFdReader.raw_string_from nfd 4 >>= fun b0 ->
  (*Lwt_io.printlf "b0:%S%!" b0 >>= fun () -> *)
  begin
    if b0 <> Asd_protocol._MAGIC
    then Lwt.fail_with "protocol error: no magic"
    else
      begin
        Llio2.NetFdReader.int32_from nfd >>= fun version ->
        if Asd_protocol.incompatible version
        then
          let open Asd_protocol.Protocol in
          let msg =
            Printf.sprintf
              "asd_protocol version: (server) %li <> %li (client)"
              _VERSION version
          in
          Lwt_log.debug msg >>= fun () ->
          let open Llio2.WriteBuffer in
          let rbuf = make ~length:32 in
          let err = Error.ProtocolVersionMismatch msg in
          Error.serialize rbuf err;
          Net_fd.write_all_lwt_bytes nfd rbuf.buf 0 rbuf.pos
        else
          begin
            Llio2.NetFdReader.option_from
              Llio2.NetFdReader.string_from
              nfd >>= fun lido ->
            Net_fd.write_all'
              nfd
              (serialize
                 (Llio.pair_to
                    Llio.int32_to
                    Llio.string_to)
                 (0l, asd_id)
              )
            >>= fun () ->
            match lido with
            | Some asd_id' when asd_id' <> asd_id -> Lwt.return ()
            | _ -> inner ()
          end
      end
  end

class check_garbage_from_advancer check_garbage_from kv =
  object
    val mutable check_garbage_from = check_garbage_from
    val mutable mem = Int64Set.empty

    method release fnr =
      mem <- Int64Set.add fnr mem;
      if Int64.(check_garbage_from =: fnr)
      then begin
        let rec inner () =
          let first_o =
            try Some (Int64Set.min_elt mem)
            with Not_found -> None in
          match first_o with
          | None -> ()
          | Some first ->
            if Int64.(first =: check_garbage_from)
            then begin
              mem <- Int64Set.remove fnr mem;
              check_garbage_from <- Int64.succ check_garbage_from;
              inner ()
            end
        in
        inner ();
        Rocks.RocksDb.put
          kv
          wo_no_wal
          Keys.check_garbage_from
          (serialize Llio.int64_to check_garbage_from)
      end
  end

let run_server
      ?cancel
      ?(write_blobs = true)
      (hosts:string list)
      (port:int option)
      ~(transport: Net_fd.transport)
      (path:string)
      ~asd_id ~node_id
      ~fsync ~slow
      ~rocksdb_max_open_files
      ~rocksdb_recycle_log_file_num
      ~rocksdb_block_cache_size
      ~limit
      ~capacity
      ~multicast
      ~tls_config
      ~tcp_keepalive
      ~use_fadvise
      ~use_fallocate
      ~engine
  =

  let fsync =
    (* write_blobs=false only works when fsync is false.
     * considering you WILL already have dataloss when using
     * this configuration it should not be a problem
     * that we override whatever value was already set for fsync.
     *)
    if write_blobs
    then fsync
    else false
  in

  Lwt_log.info_f "asd_server version:%s" Alba_version.git_revision     >>= fun () ->
  Lwt_log.debug_f "tls:%s" ([%show: Asd_config.Config.tls option] tls_config) >>= fun () ->
  Lwt_log.debug_f "transport:%s" ([%show: Net_fd.transport] transport) >>= fun () ->
  let tls =
    match tls_config with
    | None -> (None: Tls.t option)
    | Some tls_config -> failwith "todo"
  in
  let db_path = path ^ "/db" in
  Lwt_log.debug_f "opening rocksdb in %S" db_path >>= fun () ->
  let db =
    Rocks_key_value_store.create'
      ~max_open_files:rocksdb_max_open_files
      ?recycle_log_file_num:rocksdb_recycle_log_file_num
      ~block_cache_size:(
        match rocksdb_block_cache_size with
        | None ->
           (* assuming 1MB fragments
            * we write multiple key value pairs to rocksdb per fragment,
            * but only one of these should be cached.
            * This key value pair has the following size:
            * - key = 1 (asd) + 1+4+1+4 (namespace prefix) + 1+4+32+4+4+4 (object/fragment identification) = 60 bytes
            * - value = 1+4 (crc32) + 1+8+4 (fnr+size) = 18
            *
            * assuming another 8 bytes of overhead by rocksdb then we get in total 86 bytes
            * that should be cached per fragment
            *
            * some notes:
            * - block cache is uncompressed
            * - there are other keys (gc tag) that also pollute the cache when deleting them
            * - we can still optimize the object_id size from 32 bytes down to 8
            *
            * so let's go for 100 bytes per 1_000_000 of capacity
            *)
           (Int64.to_float !capacity) /. 10_000.
           |> int_of_float
        | Some v -> v)
      ~db_path ()
  in
  Lwt_log.debug_f "opened rocksdb in %S" db_path >>= fun () ->
  let endgame () =
    Lwt_log.fatal_f "endgame: closing %s" db_path >>= fun () ->
    Lwt_io.printlf "endgame%!" >>= fun () ->
    let () = let open Rocks in RocksDb.close db in
    let () = Blob_access_factory.endgame () in
    Lwt_log.fatal_f "endgame: closed  %s" db_path
  in

  let asd_id = check_asd_id db asd_id in
  let () = check_node_id db node_id in

  Lwt_log.debug_f "starting asd: %S" asd_id >>= fun () ->

  let check_garbage_from =
    match Rocks.RocksDb.get db ro Keys.check_garbage_from with
    | None -> 0L
    | Some v -> deserialize Llio.int64_from v
  in

  let files_path = path ^ "/blobs" in

  Lwt.catch
    (fun () -> Lwt_extra2.create_dir files_path)
    (function
      | Unix.Unix_error (Unix.EEXIST, _, _) -> Lwt.return ()
      | exn -> Lwt.fail exn) >>= fun () ->
  let stats = AsdStatistics.make () in

  let dir_info = make_directory_info
                   ~engine ~statistics:stats
                   ~write_blobs ~use_fallocate ~use_fadvise files_path in

  let parse_filename_to_fnr name =
    try
      let fnr = Scanf.sscanf name "%Lx" Std.id in
      let file_name = Fnr.get_file_name fnr in
      if file_name = name
      then Some fnr
      else None
    with Scanf.Scan_failure _ -> None
  in

  (* need to start the thread now, otherwise collecting garbage
     during startup may hang *)
  let io_sched =
    make
      (let wb = Rocks.WriteBatch.create_no_gc () in
       fun () ->
       (* syncing an empty batch should sync all previous batches
        * written to the WAL, see
        * https://www.facebook.com/groups/rocksdb.dev/permalink/846034015495114/
        *)
       if fsync
       then
         Rocks.RocksDb.write db wo_sync wb
      )
      (if write_blobs
       then
         (fun fnr blob ~post_write ->
          dir_info # write_blob
            ~post_write
            fnr
            blob
            ~sync_parent_dirs:fsync
         )
       else
         (fun fnr blob ~post_write ->
           (* post_write still needs to be called *)
           post_write None 0 ""
         )
      )
  in
  Lwt_unix.openfile path [Lwt_unix.O_RDONLY] 0o644 >>= fun fs_fd ->
  let io_sched_t = run io_sched ~fsync ~fs_fd in

  let collect_leaf_dir leaf_dir =
    Lwt_log.debug_f "Collect leaf dir %s" leaf_dir >>= fun () ->
    Lwt_extra2.get_files_of_directory leaf_dir >>= fun files ->
    Lwt_list.iter_p
      (fun filename ->
         match parse_filename_to_fnr filename with
         | None ->
           Lwt_log.info_f
             "Found unexpected file %s inside %s which probably shouldn't be there."
             filename leaf_dir
         | Some fnr ->
           maybe_delete_file db dir_info fnr)
      files
  in

  let check_from_dir, check_from_file, check_from_path =
    dir_info # _get_file_dir_name_path check_garbage_from
  in

  let rec collect_all_sub_dirs dir = function
    | 0 -> collect_leaf_dir dir
    | n ->
      (* recursive descent until we're at the leaf dirs *)
      Lwt_log.debug_f "Collect all sub dirs of %s" dir >>= fun () ->
      Lwt_extra2.get_files_of_directory dir >>= fun sub_dirs ->
      Lwt_list.iter_s
        (fun sub_dir ->
           collect_all_sub_dirs
             (Filename.concat dir sub_dir)
             (n - 1))
        sub_dirs
  in

  let rec collect_sub_dirs dir = function
    | [] -> collect_leaf_dir dir
    | d :: ds ->
      (* scan content, recursive call for all sub_dirs >= d *)
      Lwt_log.debug_f "Collect sub dirs of %s" dir >>= fun () ->
      Lwt_extra2.get_files_of_directory dir >>= fun sub_dirs ->
      Lwt_list.iter_s
        (fun sub_dir ->
           Lwt_log.debug_f "Maybe collect %s ?" sub_dir >>= fun () ->
           let open Compare in
           match String.compare' sub_dir d with
           | LT -> Lwt.return ()
           | EQ ->
             collect_sub_dirs (Filename.concat dir sub_dir) ds
           | GT ->
             collect_all_sub_dirs
               (Filename.concat dir sub_dir)
               (List.length ds))
        sub_dirs
  in

  Lwt_log.debug_f "check_from_dir = %s" check_from_dir >>= fun () ->
  collect_sub_dirs
    files_path
    (Str.split (Str.regexp "/") check_from_dir) >>= fun () ->


  (* delete files that are still marked as to delete *)
  let (_, fnrs_to_delete), _ =
    Rocks_key_value_store.map_range
      db
      ~first:Keys.to_be_deleted_first ~finc:true
      ~last:Keys.to_be_deleted_last
      ~max:(-1)
      ~reverse:false
      (fun cur key ->
         let _, fnr =
           deserialize
             (Llio.pair_from
                Llio.char_from
                Llio.int64_from)
             key
         in
         fnr)
  in
  cleanup_files_to_delete true io_sched db dir_info fnrs_to_delete >>= fun () ->

  (* do range query on rocksdb to get biggest fnr currently in use *)
  let next_fnr =
    let inner =
      Rocks_key_value_store.
        (with_cursor
           db
           (fun cur ->
              if cur_jump cur Left Keys.file_to_key_mapping_next_prefix
              then Some (cur_get_key cur)
              else None
           )) in
    match inner with
    | None -> 0L
    | Some key ->
      if String.length key > 0 && key.[0] = Keys.file_to_key_mapping_prefix
      then begin
        let last_fnr = Keys.file_to_key_extract_fnr key in
        Int64.succ last_fnr
      end else
        0L
  in

  (* store next_fnr in rocksdb *)
  Rocks.RocksDb.put
    db
    wo_sync
    Keys.check_garbage_from
    (serialize Llio.int64_to next_fnr);

  (* this isn't strictly needed but it ensures that all
     the recovery work should soon be saved into rocksdb *)
  Rocks.FlushOptions.with_t
    (fun fo ->
       Rocks.FlushOptions.set_wait fo false;
       Rocks.RocksDb.flush db fo);

  let get_next_fnr =
    let counter = ref (Int64.pred next_fnr) in
    fun () ->
      counter := Int64.succ !counter;

      (* this ensures directories are created before they are actually needed *)
      if 0L = Int64.rem !counter 256L
      then
        Lwt.ignore_result begin
          let dir, _, _ =
            dir_info # _get_file_dir_name_path
              (Int64.add !counter 256L) in
          dir_info # ensure_dir_exists dir ~sync:fsync
        end;

      !counter
  in

  let advancer = new check_garbage_from_advancer next_fnr db in


  let latest_disk_usage =
    match Rocks.RocksDb.get
            db
            ro
            Keys.disk_usage with
    | Some v -> deserialize Llio.int64_from v
    | None ->
       (* calculate initial disk usage in upgrade scenario... *)
       let (_, disk_usage), _ =
         Rocks_key_value_store.fold_range
           db
           ~first:(Keys.key_with_public_prefix (Slice.wrap_string "")) ~finc:true
           ~last:Keys.public_key_next_prefix
           ~reverse:false ~max:(-1)
           (fun cur _ _ acc ->
            let _cs, value = deserialize
                               Value.from_buffer
                               (Rocks_key_value_store.cur_get_value cur) in
            let size = Value.get_size value in
            Int64.(add acc (of_int size))
           )
           0L
       in
       let () =
         Rocks.RocksDb.put
           db
           wo_sync
           Keys.disk_usage
           (serialize Llio.int64_to disk_usage)
       in
       disk_usage
  in
  let mgmt = AsdMgmt.make ~latest_disk_usage
                          ~capacity
                          ~limit
  in

  let protocol nfd =
    asd_protocol
      ?cancel
      db
      ~release_fnr:(fun fnr -> advancer # release fnr)
      ~slow
      io_sched
      dir_info
      stats
      ~mgmt
      ~get_next_fnr
      asd_id nfd
  in
  let maybe_add_plain_server threads =
    match port with
    | None -> threads
    | Some port ->
       let t = Networking2.make_server
                 ?cancel
                 hosts port ~transport
                 ~tcp_keepalive
                 ~tls:None protocol
       in
       t :: threads
  in
  let maybe_add_tls_server threads =
      match tls with
      | None -> threads
      | Some _tls ->
         let open Asd_config.Config in
         let tlsPort = let cfg = Option.get_some tls_config in cfg.port in
         assert (transport = Net_fd.TCP);
         let t = Networking2.make_server ?cancel hosts tlsPort
                                         ~transport ~tcp_keepalive ~tls protocol
         in
         t :: threads
  in
  let maybe_add_multicast threads =
    match multicast with
    | None -> threads
    | Some mcast_period ->
       let tlsPort =
         if tls = None
         then None
         else
           let open Asd_config.Config in
           let tlsPort = let cfg = Option.get_some tls_config in cfg.port in
           Some tlsPort
       in
       let mcast_t () =
         let disk_usage () =
           let open Asd_protocol.AsdMgmt in
           Lwt.return
             (mgmt.latest_disk_usage,
              !(mgmt.capacity))
         in
         let useRdma =
           match transport with
           | Net_fd.TCP  -> None
           | Net_fd.RDMA -> Some true
         in
         Discovery.multicast
           asd_id node_id
           hosts ~port ~tlsPort ~useRdma
           mcast_period
           ~disk_usage
       in
       mcast_t () :: threads
  in
  let reporting_t =
    let section = AsdStatistics.section in
    Mem_stats.reporting_t
      ~section
      ~f:(fun () ->
          Lwt_log.info_f
            ~section "%s"
            (AsdStatistics.show_inner
               stats
               stats_tag_to_string
            )
      )
      ()
  in
  let threads =
    [
      (Lwt_extra2.make_fuse_thread ());
      reporting_t;
      io_sched_t;
    ]
    |> maybe_add_plain_server
    |> maybe_add_tls_server
    |> maybe_add_multicast
  in
  let t = Lwt.pick threads
  in
  Lwt.finalize
    (fun () ->
       Lwt.catch
         (fun () -> t)
         (fun exn ->
            Lwt_log.warning_f
              ~exn
              "Going down after unexpected exception in asd process" >>= fun () ->
            Lwt.fail exn))
    endgame
