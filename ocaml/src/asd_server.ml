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

open Lwt.Infix
open Prelude
open Rocks_store
open Asd_protocol
open Slice
open Checksum
open Asd_statistics
open Asd_io_scheduler
open Lwt_bytes2

let blob_threshold = 16 * 1024

module KVS = Key_value_store

module Keys = struct
  let node_id = "*node_id"
  let asd_id = "*asd_id"

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

module DirectoryInfo = struct
  type directory_status =
    | Exists
    | Creating of unit Lwt.t

  type t = {
      files_path : string;
      directory_cache : (string, directory_status) Hashtbl.t;
      write_blobs : bool;
      use_fadvise: bool;
      use_fallocate: bool
    }

  let make ?(write_blobs = true)
           ~use_fadvise
           ~use_fallocate
           files_path
    =
    if files_path.[0] <> '/'
    then
      failwith (Printf.sprintf "'%s' should be an absolute path" files_path);
    let directory_cache = Hashtbl.create 3 in
    Hashtbl.add directory_cache "." Exists;
    { files_path;
      directory_cache;
      write_blobs;
      use_fadvise;
      use_fallocate;
    }

  let get_file_name fnr =
    Printf.sprintf "%016Lx" fnr

  let get_file_dir_name_path t fnr =
    let file = get_file_name fnr in
    let dir = Bytes.create 20 in
    let rec fill off1 off2 =
      if off1 < 20
      then begin
        Bytes.set dir off1 file.[off2];
        if off2 mod 2 = 1
        then begin
          if off1 <> 19
          then Bytes.set dir (off1 + 1) '/';
          fill (off1 + 2) (off2 + 1)
        end else fill (off1 + 1) (off2 + 1)
      end
    in
    fill 0 0;
    let path =
      String.concat
        Filename.dir_sep
        [ t.files_path; dir; file ] in
    dir, file, path

  let get_file_path t fnr =
    let _, _, path = get_file_dir_name_path t fnr in
    path

  let with_blob_fd t fnr f =
    Lwt_extra2.with_fd
      (if t.write_blobs
       then get_file_path t fnr
       else "/dev/zero")
      ~flags:Lwt_unix.([O_RDONLY;])
      ~perm:0600
      f

  let get_blob t fnr size =
    Lwt_log.debug_f "getting blob %Li with size %i" fnr size >>= fun () ->
    let bs = Bytes.create size in
    with_blob_fd
      t fnr
      (fun fd ->
         Lwt_extra2.read_all fd bs 0 size >>= fun got ->
         assert (got = size);
         Lwt.return ()) >>= fun () ->
    Lwt_log.debug_f "got blob %Li" fnr >>= fun () ->
    Lwt.return bs

  let rec ensure_dir_exists t dir ~sync =
    Lwt_log.debug_f "ensure_dir_exists: %s" dir >>= fun () ->
    match Hashtbl.find t.directory_cache dir with
    | Exists -> Lwt.return ()
    | Creating wait -> wait
    | exception Not_found ->
      let sleep, awake = Lwt.wait () in
      (* the sleeper should be woken up under all
         circumstances, hence the exception handling
         below.
         (otherwise this could e.g.
          block the fragment cache...)
       *)

      Lwt.catch
        (fun () ->
           Hashtbl.add t.directory_cache dir (Creating sleep);

           let parent_dir = Filename.dirname dir in
           ensure_dir_exists t parent_dir ~sync >>= fun () ->

           Lwt_extra2.create_dir
             ~sync
             (Filename.concat t.files_path dir))
        (function
          | Unix.Unix_error (Unix.EEXIST, _, _) ->
            Lwt.return ()
          | exn ->
            Hashtbl.remove t.directory_cache dir;
            (* need to wake up the waiter here so it doesn't wait forever *)
            Lwt.wakeup_exn awake exn;
            Lwt.fail exn) >>= fun () ->

      Hashtbl.replace t.directory_cache dir Exists;
      Lwt.wakeup awake ();

      Lwt.return ()

  let delete_dir t = function
    | "."
    | "" -> Lwt.fail_with "just don't"
    | dir ->
      begin
        let full_dir = Filename.concat t.files_path dir in
        match Hashtbl.find t.directory_cache dir with
        | Exists ->
           Hashtbl.remove t.directory_cache dir;
           Lwt_unix.rmdir full_dir
        | Creating wait -> Lwt.fail_with (Printf.sprintf "creating %s" dir)
        | exception Not_found ->
                    Lwt_unix.rmdir full_dir
      end


  let write_blob
        t fnr blob
        ~(post_write:post_write) 
        ~sync_parent_dirs =
    Lwt_log.debug_f "writing blob %Li (use_fadvise:%b use_fallocate:%b)"
                    fnr t.use_fadvise t.use_fallocate
    >>= fun () ->
    with_timing_lwt
      (fun () ->
         let dir, _, file_path = get_file_dir_name_path t fnr in
         ensure_dir_exists t dir ~sync:sync_parent_dirs >>= fun () ->
         Lwt_extra2.with_fd
           file_path
           ~flags:Lwt_unix.([ O_WRONLY; O_CREAT; O_EXCL; ])
           ~perm:0o664
           (fun fd ->
             let open Blob in
             let len = Blob.length blob in
             (if t.use_fallocate
              then
                Posix.lwt_fallocate fd 0 0 len
              else
                Lwt.return_unit
             ) >>= fun () ->

             (* TODO push to blob module? *)
             (match blob with
              | Lwt_bytes s ->
                 Lwt_extra2.write_all_lwt_bytes
                   fd
                   s 0 len
              | Bigslice s ->
                 let open Bigstring_slice in
                 Lwt_extra2.write_all_lwt_bytes
                   fd
                   s.bs s.offset s.length
              | Bytes s ->
                 Lwt_extra2.write_all
                   fd
                   s 0 len
              | Slice s ->
                 let open Slice in
                 Lwt_extra2.write_all
                   fd
                   s.buf s.offset len
             )
             >>= fun () ->
             let parent_dir = t.files_path ^ "/" ^ dir in
             post_write fd len parent_dir
             >>= fun () ->
             let ufd = Lwt_unix.unix_file_descr fd in
             let () = if t.use_fadvise then Posix.posix_fadvise ufd 0 len Posix.POSIX_FADV_DONTNEED in
             Lwt.return_unit
           )
      )
    >>= fun (t_write, ()) ->

    (if t_write > 0.5
     then Lwt_log.info_f
     else Lwt_log.debug_f)
      "written blob %Li, took %f" fnr t_write

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
      DirectoryInfo.get_blob dir_info fnr size >>=
      compose Lwt.return Slice.wrap_string

  let get_cs = fst
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

module Net_fd = struct
  include Net_fd
  (*
    we don't want this to end up in the arakoon plugins,
    where it's needed nor used
    and pulls along cstruct and friends
   *)
  let sendfile_all ~fd_in ~(fd_out:t) size =
    match fd_out with
    | Plain fd ->
       Fsutil.sendfile_all
         ~fd_in
         ~fd_out:fd
         size
    | SSL(_,socket) ->
       let copy_using buffer =
         let buffer_size = Lwt_bytes.length buffer in
          let write_all socket buffer offset length =
            let write_from_source = Lwt_ssl.write_bytes socket buffer in
            Lwt_extra2._write_all write_from_source offset length
          in
          let rec loop todo =
            if todo = 0
            then Lwt.return_unit
            else
              begin
                let step =
                  if todo <= buffer_size
                  then todo
                  else buffer_size
                in
                Lwt_bytes.read fd_in        buffer 0 step >>= fun bytes_read ->
                write_all socket buffer 0 bytes_read >>= fun () ->
                loop (todo - bytes_read)
              end
          in
          loop size
       in
       Buffer_pool.with_buffer Buffer_pool.default_buffer_pool copy_using
end

let execute_query : type req res.
                         Rocks_key_value_store.t ->
                         (Llio2.WriteBuffer.t * (Net_fd.t ->
                                                 unit Lwt.t)) Asd_io_scheduler.t ->
                         DirectoryInfo.t ->
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
         Lwt_log.ign_debug_f "MultiGet for %s" ([%show: Slice.t list] keys);
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
      if not dir_info.DirectoryInfo.write_blobs
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
         Lwt_log.ign_debug_f "MultiGet2 for %s" ([%show: Slice.t list] keys);
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
              (fun (fnr, size) ->
               DirectoryInfo.with_blob_fd
                 dir_info fnr
                 (fun blob_fd ->
                   let blob_ufd = Lwt_unix.unix_file_descr blob_fd in
                   let () =
                     if dir_info.DirectoryInfo.use_fadvise
                     then Posix.posix_fadvise blob_ufd 0 size Posix.POSIX_FADV_SEQUENTIAL
                   in
                   Net_fd.sendfile_all
                     ~fd_in:blob_fd
                     ~fd_out:nfd
                     size
                   >>= fun () ->
                   let () =
                     if dir_info.DirectoryInfo.use_fadvise
                     then Posix.posix_fadvise blob_ufd 0 size Posix.POSIX_FADV_DONTNEED
                   in
                   Lwt.return_unit
                 )
              )
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
    | Statistics -> fun clear ->
                    Asd_statistics.AsdStatistics.snapshot stats clear |> return'
    | GetVersion -> fun () ->
                    return' Alba_version.summary
    | GetDiskUsage ->
       fun () ->
       return' !(mgmt.AsdMgmt.latest_disk_usage)

exception ConcurrentModification

let cleanup_files_to_delete ignore_unlink_error io_sched kv dir_info fnrs =
  if fnrs = []
  then Lwt.return ()
  else begin
    let fnrs = List.sort Int64.compare fnrs in
    Lwt_list.iter_s
      (fun fnr ->
         let path = DirectoryInfo.get_file_path dir_info fnr in

         (* TODO bulk sync of (unique) parent filedescriptors *)
         Lwt_extra2.unlink
           ~may_not_exist:(ignore_unlink_error || not dir_info.DirectoryInfo.write_blobs)
           ~fsync_parent_dir:true
           path)
      fnrs >>= fun () ->

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
    let file_path = DirectoryInfo.get_file_path dir_info fnr in
    Lwt_extra2.unlink ~fsync_parent_dir:true file_path >>= fun () ->
    Rocks.RocksDb.delete
      kv
      wo_no_wal
      (Keys.to_be_deleted fnr);
    Lwt.return ()


let execute_update : type req res.
  Rocks_key_value_store.t ->
  release_fnr : (int64 -> unit) ->
  (Llio2.WriteBuffer.t * (Net_fd.t ->
                          unit Lwt.t)) Asd_io_scheduler.t ->
  DirectoryInfo.t ->
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

        let with_immediate_updates_promise f =

          let files = ref [] in
          let allow_getting_file = ref true in
          let get_file_path () =
            if !allow_getting_file
            then begin
                let fnr = get_next_fnr () in
                let file_path = DirectoryInfo.get_file_path dir_info fnr in
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
             f immediate_upds_promise >>= fun () ->

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
             let files_to_delete =
               List.fold_left
                 (fun acc (key, action) ->
                  let acc' = match get_value_option kv key with
                    | None
                    | Some (_, Value.Direct _) -> acc
                    | Some (_, Value.OnFs (fnr, _)) ->
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

                       fnr :: acc
                  in
                  let () = match action with
                    | `Set (value, value') ->
                       begin
                         let open Value in
                         match value with
                         | Direct _ -> ()
                         | OnFs (fnr, _) ->
                            let file_map_key = Keys.file_to_key_mapping fnr in
                            let open Slice in
                            WriteBatch.put_slice
                              wb
                              file_map_key 0 (String.length file_map_key)
                              key.buf key.offset key.length
                       end;
                       let open Slice in
                       with_prefix_byte_unsafe
                         key
                         Keys.public_prefix_byte
                         (fun key' ->
                          WriteBatch.put_slice
                            wb
                            key'.buf key'.offset key'.length
                            value' 0 (String.length value'))
                    | `Delete ->
                       let open Slice in
                       with_prefix_byte_unsafe
                         key
                         Keys.public_prefix_byte
                         (fun key' ->
                          WriteBatch.delete_slice
                            wb
                            key'.buf key'.offset key'.length)
                  in
                  acc')
                 []
                 immediate_updates in
             RocksDb.write kv wo_no_sync wb;

             files_to_delete)
        in

        with_immediate_updates_promise
          (fun immediate_updates_promise ->
           let rec inner = function
             | 5 -> Lwt.fail ConcurrentModification
             | attempt ->
                Lwt.catch
                  (fun () ->
                   transform_asserts () >>= fun (none_asserts, some_asserts) ->
                   immediate_updates_promise >>= fun immediate_updates ->
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
    Net_fd.write_all_lwt_bytes res.Llio.buf 0 res.Llio.pos nfd >>= fun () ->
    write_extra nfd
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
      "Request %s took %f" (Protocol.code_to_description code) delta >>= fun () ->
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
          Net_fd.write_all_lwt_bytes
            rbuf.buf 0 rbuf.pos
            nfd
        else
          begin
            Llio2.NetFdReader.option_from
              Llio2.NetFdReader.string_from
              nfd >>= fun lido ->
            Net_fd.write_all
              (serialize
                 (Llio.pair_to
                    Llio.int32_to
                    Llio.string_to)
                 (0l, asd_id)
              )
              nfd
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
      (path:string)
      ~asd_id ~node_id
      ~fsync ~slow
      ~buffer_size
      ~rocksdb_max_open_files
      ~limit
      ~multicast
      ~tls
      ~tcp_keepalive
      ~use_fadvise
      ~use_fallocate
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
  Lwt_log.debug_f "tls:%s" ([%show: Asd_config.Config.tls option] tls) >>= fun () ->

  let ctx = Tls.to_server_context tls in
  let db_path = path ^ "/db" in
  Lwt_log.debug_f "opening rocksdb in %S" db_path >>= fun () ->
  let kv =
    Rocks_key_value_store.create'
      ~max_open_files:rocksdb_max_open_files
      ~db_path ()
  in
  Lwt_log.debug_f "opened rocksdb in %S" db_path >>= fun () ->
  let endgame () =
    Lwt_log.fatal_f "endgame: closing %s" db_path >>= fun () ->
    Lwt_io.printlf "endgame%!" >>= fun () ->
    let () = let open Rocks in RocksDb.close kv in
    Lwt_log.fatal_f "endgame: closed  %s" db_path
  in

  let asd_id = check_asd_id kv asd_id in
  let () = check_node_id kv node_id in

  Lwt_log.debug_f "starting asd: %S" asd_id >>= fun () ->

  let check_garbage_from =
    match Rocks.RocksDb.get kv ro Keys.check_garbage_from with
    | None -> 0L
    | Some v -> deserialize Llio.int64_from v
  in

  let files_path = path ^ "/blobs" in

  Lwt.catch
    (fun () -> Lwt_extra2.create_dir files_path)
    (function
      | Unix.Unix_error (Unix.EEXIST, _, _) -> Lwt.return ()
      | exn -> Lwt.fail exn) >>= fun () ->

  let dir_info = DirectoryInfo.make ~write_blobs ~use_fallocate ~use_fadvise files_path in

  let parse_filename_to_fnr name =
    try
      let fnr = Scanf.sscanf name "%Lx" Std.id in
      let file_name = DirectoryInfo.get_file_name fnr in
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
         Rocks.RocksDb.write kv wo_sync wb
      )
      (if write_blobs
       then
         (fun fnr blob post_write ->
          DirectoryInfo.write_blob
            ~post_write
            dir_info
            fnr
            blob
            ~sync_parent_dirs:fsync
         )
       else
         (fun fnr blob post_write ->
          Lwt.return_unit)
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
           maybe_delete_file kv dir_info fnr)
      files
  in

  let check_from_dir, check_from_file, check_from_path =
    DirectoryInfo.get_file_dir_name_path
      dir_info
      check_garbage_from
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
      kv
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
  cleanup_files_to_delete true io_sched kv dir_info fnrs_to_delete >>= fun () ->

  (* do range query on rocksdb to get biggest fnr currently in use *)
  let next_fnr =
    let inner =
      Rocks_key_value_store.
        (with_cursor
           kv
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
    kv
    wo_sync
    Keys.check_garbage_from
    (serialize Llio.int64_to next_fnr);

  (* this isn't strictly needed but it ensures that all
     the recovery work should soon be saved into rocksdb *)
  Rocks.FlushOptions.with_t
    (fun fo ->
       Rocks.FlushOptions.set_wait fo false;
       Rocks.RocksDb.flush kv fo);

  let get_next_fnr =
    let counter = ref (Int64.pred next_fnr) in
    fun () ->
      counter := Int64.succ !counter;

      (* this ensures directories are created before they are actually needed *)
      if 0L = Int64.rem !counter 256L
      then
        Lwt.ignore_result begin
          let dir, _, _ =
            DirectoryInfo.get_file_dir_name_path
              dir_info
              (Int64.add !counter 256L) in
          DirectoryInfo.ensure_dir_exists dir_info dir ~sync:fsync
        end;

      !counter
  in

  let advancer = new check_garbage_from_advancer next_fnr kv in

  let stats = AsdStatistics.make () in
  let latest_disk_usage = ref (Fsutil.disk_usage path) in
  let disk_usage () =
    Fsutil.lwt_disk_usage path >>= fun r ->
    let () = latest_disk_usage := r in
    Lwt.return r
  in
  let mgmt = AsdMgmt.make latest_disk_usage limit in

  let protocol nfd =
    asd_protocol
      ?cancel
      kv
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
                 hosts port
                 ~tcp_keepalive
                 ~ctx:None protocol
       in
       t :: threads
  in
  let maybe_add_tls_server threads =
      match tls with
      | None -> threads
      | Some tls ->
         let open Asd_config.Config in
         let tlsPort = tls.port in
         let t = Networking2.make_server ?cancel hosts tlsPort ~tcp_keepalive ~ctx protocol in
         t :: threads
  in
  let maybe_add_multicast threads =
    match multicast with
    | None -> threads
    | Some mcast_period ->
       let tlsPort =
         match tls with
         | None   -> None
         | Some tls -> let open Asd_config.Config in Some tls.port
       in
       let mcast_t () =
         Discovery.multicast asd_id node_id hosts port tlsPort mcast_period
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
               Asd_protocol.Protocol.code_to_description))
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
