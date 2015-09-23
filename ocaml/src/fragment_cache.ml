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
open Unix
open Lwt.Infix
open Rocks_store
module KV = Rocks_key_value_store

class type cache = object
    method clear_all : unit -> unit Lwt.t
    method add : int32 -> string -> Bytes.t -> unit Lwt.t
    method lookup : int32 -> string -> bytes option Lwt.t

    method drop : int32 -> unit Lwt.t
    method get_count : unit -> int64
    method get_total_size : unit -> int64
    method close : unit -> unit Lwt.t
end

class no_cache = object(self :# cache)
    method clear_all () = Lwt.return ()
    method add    bid oid blob = Lwt.return ()
    method lookup bid oid      = Lwt.return None
    method drop   bid          = Lwt.return ()
    method get_count () = 0L
    method get_total_size () = 0L
    method close () = Lwt.return ()
end

let ser64 x=
  let buffer = Buffer.create 8 in
  Llio.int64_to buffer x;
  Buffer.contents buffer

let ser64_be x =
  let buffer = Buffer.create 8  in
  Llio.int64_be_to buffer x;
  Buffer.contents buffer

let (+:) = Int64.add
let (-:) = Int64.sub

let marker_name root = Printf.sprintf "%s/alba_proxy_marker" root

let get_int64 db key =
  match KV.get db key with
  | None -> 0L
  | Some v -> Llio.int64_from (Llio.make_buffer v 0)

let get_int32_exn db key =
  let v = KV.get_exn db key in
  let b = Llio.make_buffer v 0  in
  Llio.int_from b


let read_it fd ~len =
      let buffer = Bytes.create len in
      Lwt_extra2.read_all fd buffer 0 len >>= fun size' ->
      assert (size' = len);
      Lwt.return buffer

let rm_tree ~silent dir =
      let cmd = Printf.sprintf "rm -rf %s" dir in
      Lwt_log.debug_f "cmd:%S" cmd >>= fun () ->
      let command = Lwt_process.shell cmd in
      Lwt_process.exec command >>= fun status ->
      match status with
      | Unix.WEXITED 0 -> Lwt_log.debug_f "cmd:%S ok" cmd
      | Unix.WEXITED x ->
         let msg = Printf.sprintf "rm_tree %S status:%i" dir x in
         Lwt_log.warning msg >>= fun () ->
         if silent
         then Lwt.return ()
         else Lwt.fail_with msg
      | _ -> (* killed by signal or really weird shit *)
         Lwt_log.warning_f "rm_tree failed " >>= fun () ->
         if silent
         then Lwt.return()
         else Lwt.fail_with (Printf.sprintf "rm_tree : %s" dir)


type boid = int32 * string [@@deriving show]


class blob_cache root ~(max_size:int64) ~rocksdb_max_open_files =
  let _TOTAL_COUNT = "*total_count"
  and _TOTAL_SIZE = "*total_size"
  and _BLOBS  = "/blobs/"
  and _BLOBS' = "/blobs0"
  and _LRU = "/lru/"
  and _FSID = "00_fsid"
  and _SIZE = "10_size"
  and _ACCESS = "20_access"
  (* set of bid-s which are being dropped *)
  and _dropping = Hashtbl.create 3
  and section = Lwt_log.Section.make "fragment_cache"
  in

  let path_of_fsid (bid:int32) fsid =
      let b = Buffer.create 100 in
      Buffer.add_string b (Printf.sprintf "%08lx/" bid);
      Bytes.iteri
        (fun i c ->
         let h = Printf.sprintf "%02x" (int_of_char c) in
         Buffer.add_string b h;
         if i < 7
         then Buffer.add_char b '/'
        ) fsid;
      Buffer.add_string b ".blob";
      Buffer.contents b
  in

  let _blob_key boid attribute_name =
    let bid, oid = boid in
    let b = Buffer.create 120 in
    Buffer.add_string b _BLOBS;
    Llio.int32_to b bid;
    Buffer.add_string b oid;
    Buffer.add_string b attribute_name;
    Buffer.contents b
  in

  (* 'blob' index *)
  let blob_start_key_of  bid  = _blob_key (bid,"") ""  in
  let blob_fsid_key_of boid   = _blob_key boid _FSID   in
  let blob_size_key_of boid   = _blob_key boid _SIZE   in
  let blob_access_key_of boid = _blob_key boid _ACCESS in
  let blob_too_far_key_of bid =
    if bid = Int32.max_int
    then _BLOBS'
    else blob_start_key_of (Int32.succ bid)
  in
  let boid_of_fsid_key key =
    let start = String.length _BLOBS in
    let b = Llio.make_buffer key start in
    let bid = Llio.int32_from b in
    let start4 = start + 4 in
    let oid =
      String.sub
        key start4
        (String.length key - start4 - String.length _FSID)
    in
    (bid,oid)
  in
  let access_key_of access =
    let b = Buffer.create 20 in
    Buffer.add_string b _LRU;
    Llio.int64_be_to b access;
    Buffer.contents b
  in
  let access_of_access_key kaccess =
    let head = String.length _LRU in
    let sub = Bytes.sub kaccess head 8 in
    deserialize Llio.int64_be_from sub
  in
  let boid_of vboid =
    let b   = Llio.make_buffer vboid 0 in
    let bid = Llio.int32_from b in
    let oid = String.sub vboid 4 (String.length vboid -4) in
    (bid, oid)
  in
  let boid_value_of (bid,oid) =
    let b = Buffer.create (4 + Bytes.length oid) in
    let () = Llio.int32_to b bid in
    let () = Buffer.add_string b oid in
    Buffer.contents b
  in

  let size_value_of size =
    let b = Buffer.create 4 in
    let () = Llio.int_to b size in
    Buffer.contents b
  in

  let size_of vsize =
    let b = Llio.make_buffer vsize 0  in
    Llio.int_from b
  in

  let create_access =
    let i64_stamp () =
      let f0 = Unix.gettimeofday() in
      let f = f0 *. 10_000.0 in
      Int64.of_float f
    in
    let low = ref (i64_stamp ())
    in
    fun () ->
       let now = i64_stamp() in
       let r = max now (Int64.succ !low) in
       let () = low := r in
       r
  in

  let access_value_of access = ser64_be access in
  let access_of vaccess = Prelude.deserialize Llio.int64_be_from vaccess in
  let bid_of boid = fst boid in

  let canonical path = Printf.sprintf "%s/%s" root path in
  let _write_blob path fragment :unit Lwt.t=
    let _inner () =
      let full_path = canonical path in
      Lwt_log.debug_f "_write_blob: %s" full_path >>= fun () ->
      let perm = 0o644 in
      let flags = [O_WRONLY;O_CREAT] in

      Lwt_extra2.with_fd
        full_path ~flags ~perm
        (fun fd_out ->
         let fragment_l = String.length fragment in
         Lwt_extra2.write_all fd_out fragment 0 fragment_l
        )
    in
    Alba_statistics.Statistics.with_timing_lwt _inner  >>= fun (took,()) ->
    Lwt_log.debug_f "_write_blob path:%s took:%f" path took
  in

  let cleanup_needed bid fsid fsid0 =
    let path0 = path_of_fsid bid fsid0 in
    let path  = path_of_fsid bid fsid in
    let dir0 = Filename.dirname path0 in
    let dir = Filename.dirname path in
    dir0 <> dir
  in

  let db_path = root ^ "/db" in

  object(self :# cache)
    val dirs = Asd_server.DirectoryInfo.make root
    val mutable db =
      KV.create'
        ~db_path
        ~max_open_files:rocksdb_max_open_files ()

    val t0 = let ts = Unix.gettimeofday () in
             let tu = ts *. 100000.0 in
             Int64.of_float tu

    val mutable c = 0L
    val _mutex = Lwt_mutex.create ()

    method dump_lru() =
      KV.with_cursor
        db
        (fun c ->
         let _ = KV.cur_jump c Prelude.Right _LRU in
         let rec loop count =
           if Rocks.Iterator.is_valid c
           then
             let key  = KV.cur_get_key c in
             let vboid = KV.cur_get_value c in
             let boid = boid_of vboid in
             let access = access_of_access_key key in
             Printf.printf "key:%30S access:%Li boid:%s\n%!"
                           key
                           access
                           ([%show:boid] boid);
             let _ = KV.cur_next c in
             loop ()
           else ()
         in
         loop ()
        )

    method create_fs_id () =
      let c0 = c in
      let () = c  <- Int64.succ c  in
      let b=  Buffer.create 20 in
      Llio.int64_be_to b c0;
      Llio.int64_be_to b t0;
      let r = Buffer.contents b in
      r

    method get_count () = get_int64 db _TOTAL_COUNT

    method get_total_size () = get_int64 db _TOTAL_SIZE

    method clear_all ()  =
      Lwt_log.debug "blob_cache # clear_all()" >>= fun ()->
      Lwt_mutex.with_lock
        _mutex
        (fun () ->
         Lwt.catch
           (fun () ->
            let open Rocks in
            let () = RocksDb.close db in
            let dir = root ^ "/"  in
            rm_tree ~silent:false dir >>= fun () ->
            Lwt.catch
              (fun () ->Lwt_extra2.create_dir ~sync:false dir)
              (function
                | Unix.Unix_error(Unix.EEXIST, "mkdir",_ ) -> Lwt.return ()
                | exn -> Lwt.fail exn
              )
            >>= fun () ->
            db <- KV.create'
                    ~db_path
                    ~max_open_files:rocksdb_max_open_files ();
            Lwt.return ())
           (fun exn ->
            Lwt_log.warning_f ~exn "clear_all failed..." >>= fun () ->
            Lwt.return ())
        )

    method lookup bid oid =

      Hashtbl.remove _dropping bid;

      let _lookup bid oid f =
        let boid = bid,oid in
        let blob_fsid_key = blob_fsid_key_of boid in
        Lwt_log.debug_f "_lookup %lx oid:%S" bid oid >>= fun () ->
        match KV.get db blob_fsid_key with
        | None     -> Lwt.return None
        | Some ts ->
           begin
             let blob_size_key = blob_size_key_of boid in
             let blob_access_key = blob_access_key_of boid in

             let blob_size = get_int32_exn db blob_size_key in
             let boidv = boid_value_of boid in

             let accessv = KV.get_exn db blob_access_key in
             let access  = access_of accessv in
             let fsid = KV.get_exn db blob_fsid_key in
             let path = path_of_fsid bid fsid in
             let full_path = canonical path in
             let perm = 0o644 in
             let flags = [Unix.O_RDONLY] in
             Lwt_log.debug_f "full_path:%s" full_path >>= fun () ->
             Lwt.catch
               (fun () ->
                Lwt_extra2.with_fd
                  full_path ~flags ~perm
                  (fun fd -> f fd ~len:blob_size)
                >>= fun buffer ->
                Lwt_log.debug_f "read buffer from %s" full_path
                >>= fun () ->
                begin (* update access time *)
                  let access1 = create_access() in
                  let vaccess1 = access_value_of access1 in
                  let access_key1 = access_key_of access1 in
                  let access_key = access_key_of access in
                  let open Rocks in
                  WriteBatch.with_t
                    (fun wb ->
                     WriteBatch.put wb blob_access_key vaccess1;
                     WriteBatch.delete wb access_key;
                     WriteBatch.put wb access_key1 boidv;
                     WriteOptions.with_t (fun wo -> RocksDb.write db wo wb)
                    );
                end;
                Lwt_log.debug_f "lookup %lx oid:%S was a hit!" bid oid
                >>= fun () ->
                Lwt.return (Some buffer)
               )
               (fun exn ->
                Lwt_log.warning_f ~exn "during lookup (returning None)"
                >>= fun () ->
                Lwt.return None
               )
           end
      in
      Lwt_log.debug_f "lookup %lx oid:%S" bid oid >>= fun () ->
      Lwt.catch
        (fun () ->
         Lwt_mutex.with_lock
           _mutex
           (fun () -> _lookup bid oid read_it)
        )
        (fun exn ->
         Lwt_log.warning ~exn "the cache exploded. returning None" >>= fun () ->
         Lwt.return None
        )


    method _check () =
      Printf.printf "_check()\n%!";
      let iter_blob_kvs f =
        let prefix_length = String.length _BLOBS in
        KV.with_cursor
          db
          (fun c ->
           let _ = KV.cur_jump c Prelude.Right _BLOBS in
           let rec loop count =
             if Rocks.Iterator.is_valid c
             then
               let key  = KV.cur_get_key c
               and fsid = KV.cur_get_value c in
               let key_prefix = String.sub key 0 prefix_length in
               if key_prefix = _BLOBS
               then
                 begin
                   let () = f key fsid in
                   let _ = KV.cur_next c in (*size *)
                   let _ = KV.cur_next c in (*access *)
                   let _ = KV.cur_next c in
                   loop ()
                 end
               else ()
             else
               ()
           in
           loop ()
          )
      in
      let from_db () = (* for all fsids in the database, do we have a blob? *)
        let count = ref 0 in
        let check_key key fsid =
          begin
            let () =
              Printf.printf "key:%60s fsid:%48s%!"
                            (Prelude.to_hex key)
                            (Prelude.to_hex fsid)
            in
            let boid = boid_of_fsid_key key in
            let () = Printf.printf " boid:%16s %!" ([%show :boid] boid) in
            let bid = bid_of boid in
            let path = path_of_fsid bid fsid in
            let full_path = canonical path in
            let () = if Sys.file_exists full_path
                     then Printf.printf " %S exists\n%!" full_path
                     else
                       let () = Printf.printf " %S does not exist\n%!" full_path
                       in
                       failwith "check failed (1)"
            in
            incr count
          end
        in
        iter_blob_kvs check_key;
        !count |> Int64.of_int
      in

      let from_fs () =
        let is_dir f =
          let st = Unix.stat f in
          st.st_kind = Unix.S_DIR
        in
        let this_level (dir:string) =
          let rec inner h (dirs:string list) files =
            let entry = try Some (Unix.readdir h ) with End_of_file -> None in
            match entry with
            | None -> (dirs,files)
            | Some "." | Some ".." | Some "db" -> inner h dirs files
            | Some e ->
               let full = Printf.sprintf "%s/%s" dir e in
               if is_dir full
               then inner h (full::dirs) files
               else inner h dirs (full::files)
          in
          let h = Unix.opendir dir in
          let r = inner h [] [] in
          let () = closedir h in
          r
        in
        let entries dir =
          let rec walk dir acc =
            let dirs,files = this_level dir in
            List.fold_left
              (fun acc d ->
               let acc' = walk d acc in
               acc'
              ) (files @ acc) dirs
          in
          walk dir []
        in
        let blob_names = entries root in
        let size_fs =
          List.fold_left
            (fun acc f ->
             let st = Unix.stat f in
             acc + st.st_size
            )
            0
            blob_names
        in
        let count_fs = List.length blob_names in
        Printf.printf "xs=%s count_fs:%i total_size:%i\n"
                      ([%show:string list] blob_names)
                      count_fs size_fs;
        let blob_names_set = StringSet.of_list blob_names in
        let remainder_set =
          let r = ref blob_names_set in
          let () =
            iter_blob_kvs
              (fun key fsid ->
               let boid = boid_of_fsid_key key in
               let bid = bid_of boid in
               let path = path_of_fsid bid fsid in
               let full_path = canonical path in
               let new_set = StringSet.remove full_path !r in
               r := new_set
            )
          in
          ! r
        in
        count_fs |> Int64.of_int,
        size_fs  |> Int64.of_int, remainder_set
      in
      try
        (*let () = self # dump_lru() in *)
        let size_rocks = self # get_total_size () in
        let count_fs,size_fs,remainder_set  = from_fs()  in
        Printf.printf "size_fs=%Li size_rocks=%Li\n" size_fs size_rocks;
        let n_rogue_blobs = StringSet.cardinal remainder_set in
        Printf.printf "n_rogue_blobs:%i\n" n_rogue_blobs;
        StringSet.iter
          (fun bn -> Printf.printf "%S?\n" bn) remainder_set;
        let count_db = from_db() in
        let count_rocks = self # get_count ()  in

        Printf.printf
          "count_db:%Li, count_fs:%Li get_count:%Li\n%!"
          count_db count_fs count_rocks;
        (size_rocks = size_fs
         && count_fs = count_db
         && count_db = count_rocks
         && n_rogue_blobs = 0
        )

      with exn ->
        let () = Printf.printf "exn:%s\n%!" (Printexc.to_string exn) in
        false

    method _add_new_grow
             ~total_size ~total_count
             ~access ~fsid
             (boid:boid) path blob =
      Lwt_log.debug_f ~section
        ("_add_new_grow ~boid:%s ~fsid:%s " ^^
           "~total_size:%Li ~max_size:%Li access:%Li ~total_count:%Li")
        ([%show : boid] boid)
        (Prelude.to_hex fsid)
        total_size
        max_size
        access
        total_count
      >>= fun () ->
      _write_blob path blob >>= fun () ->

      let blob_length = Bytes.length blob in
      let blob_length64 = Int64.of_int blob_length in
      let total_count' = ser64 (Int64.succ total_count) in
      let total_size' = ser64 (total_size +: blob_length64) in
      let open Rocks in
      let kfsid = blob_fsid_key_of boid in
      let ksize = blob_size_key_of boid in
      let kaccess = blob_access_key_of boid in
      let kaccess_rev = access_key_of access in
      let vsize = size_value_of blob_length in
      let vaccess = access_value_of access in
      let vboid = boid_value_of boid in
      WriteBatch.with_t
        (fun wb ->
         WriteBatch.put wb kfsid fsid;
         WriteBatch.put wb ksize vsize;
         WriteBatch.put wb kaccess vaccess;
         WriteBatch.put wb kaccess_rev vboid;
         WriteBatch.put wb _TOTAL_COUNT total_count';
         WriteBatch.put wb _TOTAL_SIZE total_size';
         WriteOptions.with_t
           (fun wo ->
            RocksDb.write db wo wb
           )
        );
      Lwt.return ()

    method _replace_grow
             ~(total_size:int64)
             ~(total_count:int64)
             ~access ~fsid boid path blob
             ~fsid0
      =

      Lwt_log.debug_f ~section
        "_replace_grow ~boid:%s ~fsid:%s ~fsid0:%s"
        ([%show: boid] boid)
        (Prelude.to_hex fsid)
        (Prelude.to_hex fsid0)
      >>= fun () ->
      let bid = bid_of boid in
      let cleanup = cleanup_needed bid fsid fsid0 in
      self # _unlink boid fsid0 ~cleanup >>= fun () ->
      _write_blob path blob >>= fun () ->
      let open Rocks in
      let kfsid   = blob_fsid_key_of boid in
      let ksize   = blob_size_key_of boid in
      let kaccess = blob_access_key_of boid in
      let kaccess_rev = access_key_of access in
      let vaccess0 = KV.get_exn db kaccess in
      let access0 = access_of vaccess0 in

      let kaccess_rev0 =  access_key_of access0 in
      let blob_length = Bytes.length blob in
      let blob_length64 = Int64.of_int blob_length in
      let vsize = size_value_of blob_length in
      let vboid = boid_value_of boid in
      let vaccess = access_value_of access in
      let old_size64 = get_int32_exn db ksize |> Int64.of_int in
      let total_size' = ser64 (total_size +: blob_length64 -: old_size64) in

      WriteBatch.with_t
        (fun wb ->
         WriteBatch.put wb kfsid fsid;
         WriteBatch.put wb ksize vsize;
         WriteBatch.put wb kaccess vaccess;
         WriteBatch.put wb kaccess_rev vboid;
         WriteBatch.delete wb kaccess_rev0;
         WriteBatch.put wb _TOTAL_SIZE total_size';
         WriteOptions.with_t(fun wo -> RocksDb.write db wo wb)
        );
      Lwt.return ()

    method _unlink (boid0:boid) fsid0 ~cleanup =
      let bid0 = bid_of boid0 in
      let path0 = path_of_fsid bid0 fsid0 in
      let full_path0 = canonical path0 in
      Lwt_unix.unlink full_path0 >>= fun () ->
      (* might have been the last of it's kind *)

      let rec delete dir =
        Asd_server.DirectoryInfo.delete_dir dirs dir >>= fun () ->
        let parent = Filename.dirname dir in
        if parent = root
        then Lwt.return ()
        else delete parent
      in
      if cleanup then
        let dir = Filename.dirname path0 in
        Lwt.catch
          (fun () -> delete dir)
          (fun exn -> Lwt.return())
      else
        Lwt.return ()

    method _evict
        ~total_count ~total_size
        ~victims_size =
      let get_bid_victims bid victims_size =
        KV.with_cursor
          db
          (fun c ->
             let too_far = blob_too_far_key_of bid in

             let rec loop victims n_victims delta =
               let acc = (victims, n_victims, delta) in
               let open Rocks in
               if not (Iterator.is_valid c)
               then
                 acc
               else if delta >= victims_size
               then
                 acc
               else
                 let kboid = KV.cur_get_key c in
                 if kboid >= too_far
                 then acc
                 else begin
                   let boid = boid_of_fsid_key kboid in
                   let ksize = blob_size_key_of boid in
                   let size = get_int32_exn db ksize in

                   let kfsid = blob_fsid_key_of boid in
                   let fsid = KV.get_exn db kfsid in

                   let kaccess = blob_access_key_of boid in
                   let vaccess = KV.get_exn db kaccess in
                   let access  = access_of vaccess in
                   let kaccess_rev = access_key_of access in

                   let victim = kaccess_rev, boid, fsid, size in

                   let next_entry = _blob_key boid
                     (* "99" > all attribute keys like _ACCESS *) "99"
                   in
                   let _ = KV.cur_jump c Prelude.Right next_entry in

                   loop
                     (victim::victims)
                     (n_victims+1)
                     (delta+size)
                 end
             in
             let start = blob_start_key_of bid in
             let _start = KV.cur_jump c Right start in
             loop [] 0 0)
      in

      let get_dropping_victims victims_size =
        let rec inner victims n_victims delta =
          let acc = victims, n_victims, delta in
          if delta >= victims_size
          then acc
          else if Hashtbl.length _dropping = 0
          then acc
          else
            match Hashtbl.choose _dropping with
            | None -> acc
            | Some (bid, ()) ->
               let victims', n_victims', delta' = get_bid_victims bid victims_size in

               (* stop dropping items from this bucket if the
               bucket did not result in sufficient victims *)
               if delta' < victims_size
               then Hashtbl.remove _dropping bid;

               inner
                 (List.rev_append victims' victims)
                 (n_victims + n_victims')
                 (delta + delta')
        in
        inner [] 0 0
      in

      let get_lru_victims victims_size =
        KV.with_cursor db
          (fun c ->
           let rec loop acc n sum_size hit =
             if not hit || sum_size >= victims_size
             then acc, n, sum_size
             else
               begin
                 let kaccess_rev_i = KV.cur_get_key c in
                 let vboid   = KV.cur_get_value c in
                 let boid_i  = boid_of vboid in
                 let ksize_i = blob_size_key_of boid_i in
                 let size_i  = KV.get_exn db ksize_i |> size_of in

                 let kfsid_i = blob_fsid_key_of boid_i in
                 let fsid_i = KV.get_exn db kfsid_i in

                 let victim_i = kaccess_rev_i, boid_i, fsid_i, size_i in
                 let sum_size' = sum_size + size_i in
                 let acc' = victim_i :: acc in
                 let hit' = KV.cur_next c in
                 let n' = n+ 1 in
                 loop acc' n' sum_size' hit'
               end
           in
           let _hit = KV.cur_jump c Prelude.Right _LRU in
           assert _hit; (* it's full, we should have at least 1 *)
           loop [] 0 0 _hit
          )
      in

      let delete_victims_from_rocksdb victims =
        if victims <> []
        then
          begin
            let open Rocks in
            WriteBatch.with_t
              (fun wb ->
               let del_victim v_i =
                 let kaccess_rev_i,boid_i, fsid_i, size_i = v_i in
                 let kfsid_i   = blob_fsid_key_of   boid_i in
                 let ksize_i   = blob_size_key_of   boid_i in
                 let kaccess_i = blob_access_key_of boid_i in

                 WriteBatch.delete wb kfsid_i;
                 WriteBatch.delete wb ksize_i;
                 WriteBatch.delete wb kaccess_i;
                 WriteBatch.delete wb kaccess_rev_i;
               in
               let () = List.iter del_victim victims in

               WriteOptions.with_t (fun wo ->RocksDb.write db wo wb))
          end
      in

      let victims, n_victims, delta =
        let space_needed = victims_size in
        let victims, n_victims, delta = get_dropping_victims space_needed in

        delete_victims_from_rocksdb victims;

        let space_needed' = space_needed - delta in
        if space_needed' > 0
        then
          let victims', n_victims', delta' = get_lru_victims space_needed' in

          delete_victims_from_rocksdb victims';

          (List.append victims victims',
           n_victims + n_victims',
           delta + delta')
        else
          victims, n_victims, delta
      in

      let n_victims64 = Int64.of_int n_victims in
      let total_count' = total_count -: n_victims64 in
      let delta64 = Int64.of_int delta in
      let total_size' = total_size -: delta64 in

      let open Rocks in
      WriteBatch.with_t
        (fun wb ->
         WriteBatch.put wb _TOTAL_COUNT (ser64 total_count');
         WriteBatch.put wb _TOTAL_SIZE (ser64 total_size');
         WriteOptions.with_t (fun wo ->RocksDb.write db wo wb));

      let rec loop = function
        | [] -> Lwt.return_unit
        | v_i :: vs ->
           let kaccess_rev_i, boid_i, fsid_i, size_i = v_i in

           Lwt_log.debug_f ~section
                           "victim: %s %s"
                           ([%show : boid] boid_i)
                           (Prelude.to_hex fsid_i)
           >>= fun () ->
           self # _unlink boid_i fsid_i ~cleanup:true
           >>= fun () ->
           loop vs
      in
      loop victims >>= fun () ->

      Lwt.return (total_count', total_size')


    method _add_new_full
             ~total_size ~total_count ~(access:int64)
             ~fsid
             (boid:boid) path blob
      =
      Lwt_log.debug_f ~section
        "_add_new_full ~boid:%s ~fsid:%s ~total_size:%Li ~max_size:%Li access:%Li"
        ([%show : boid] boid) (Prelude.to_hex fsid)
        total_size
        max_size
        access
      >>= fun () ->

      _write_blob path blob >>= fun () ->
      let blob_size = Bytes.length blob |> Int64.of_int in
      let victims_size = blob_size -: (max_size -: total_size) |> Int64.to_int in
      self # _evict
        ~total_count ~total_size
        ~victims_size
      >>= fun (total_count, total_size) ->

      let blob_length   = Bytes.length blob in
      let blob_length64 = Int64.of_int blob_length in

      let open Rocks in

      Lwt_log.debug_f ~section "going to update KV" >>= fun () ->
      WriteBatch.with_t
        (fun wb ->
         let vsize = size_value_of blob_length in
         let total_size' = ser64 (total_size +: blob_length64) in
         let kfsid   = blob_fsid_key_of   boid in
         let ksize   = blob_size_key_of   boid in
         let kaccess = blob_access_key_of boid in
         let kaccess_rev = access_key_of access in
         let vboid = boid_value_of boid in
         let vaccess = access_value_of access in
         let total_count' = ser64 (total_count +: 1L) in

         WriteBatch.put wb kfsid fsid;
         WriteBatch.put wb ksize vsize;
         WriteBatch.put wb kaccess vaccess;
         WriteBatch.put wb kaccess_rev vboid;

         WriteBatch.put wb _TOTAL_COUNT total_count';
         WriteBatch.put wb _TOTAL_SIZE  total_size';
         WriteOptions.with_t (fun wo ->RocksDb.write db wo wb)
        );

      Lwt_log.debug_f ~section "after batch"

    method _replace_full
             ~(total_size:int64)
             ~(total_count:int64)
             ~access
             ~fsid
             boid
             path blob
             ~fsid0
      =
      Lwt_log.debug_f ~section
        "_replace_full ~boid:%s ~fsid:%s ~total_size:%Li ~max_size:%Li access:%Li"
        ([%show:boid] boid)
        (Prelude.to_hex fsid)
        total_size
        max_size
        access

      >>= fun () ->
      let bid = bid_of boid in
      let cleanup = cleanup_needed bid fsid fsid0 in
      self # _unlink boid fsid0 ~cleanup  >>= fun () ->
      _write_blob path blob >>= fun () ->

      let kfsid = blob_fsid_key_of boid in
      let blob_length = Bytes.length blob in
      let blob_length64 = Int64.of_int blob_length in
      let ksize   = blob_size_key_of   boid  in
      let blob0_length64 = get_int32_exn db ksize |> Int64.of_int in
      let vsize = size_value_of blob_length in
      let total_size' =
        ser64 (total_size -: blob0_length64 +: blob_length64)
      in

      let kaccess = blob_access_key_of boid  in
      let vaccess0 = KV.get_exn db kaccess in
      let access0  = access_of vaccess0 in
      let kaccess_rev = access_key_of access in
      let kaccess_rev0 = access_key_of access0 in
      let vboid = boid_value_of boid in
      let vaccess = access_value_of access in

      let open Rocks in
      WriteBatch.with_t
        (fun wb ->
         WriteBatch.put wb kfsid fsid;
         WriteBatch.put wb ksize vsize;
         WriteBatch.put wb kaccess vaccess;
         WriteBatch.put wb kaccess_rev vboid;
         WriteBatch.delete wb kaccess_rev0;
         (* total count remains the same *)
         WriteBatch.put wb _TOTAL_SIZE total_size';
         WriteOptions.with_t (fun wo -> RocksDb.write db wo wb)
        );
      Lwt.return ()

    method add bid oid blob : unit Lwt.t =

      Lwt_log.debug_f "add %lx %S" bid oid >>= fun () ->
      let _add () =
        Lwt_log.debug_f "_add %lx %S" bid oid >>= fun () ->
        let boid = (bid, oid) in

        let fsid = self # create_fs_id () in
        let fsid_key = blob_fsid_key_of boid in
        let access = create_access () in
        let path = path_of_fsid bid fsid in
        Lwt.catch
          (fun () ->
           let dir = Filename.dirname path in
           Lwt_log.debug_f "add...path=%s dir=%s" path dir
           >>= fun () ->
           Asd_server.DirectoryInfo.ensure_dir_exists dirs dir
           >>= fun () ->
           let total_count = get_int64 db _TOTAL_COUNT in
           let total_size  = get_int64 db _TOTAL_SIZE in
           let blob_length = Bytes.length blob |> Int64.of_int in
           begin
             match total_size +: blob_length < max_size ,
                   KV.get db fsid_key
             with
             | true, None      ->
                self#_add_new_grow
                  ~total_size ~total_count ~access ~fsid boid path blob
             | true, Some fsid0  ->
                self#_replace_grow
                  ~total_size ~total_count ~access ~fsid boid path blob ~fsid0

             | false, None     ->
                self#_add_new_full
                  ~total_size ~total_count ~access ~fsid boid path blob

             | false, Some fsid0 ->
                self#_replace_full
                  ~total_size ~total_count ~access ~fsid boid path blob ~fsid0

           end
          )
          (fun exn ->
           Lwt_log.warning_f ~exn
             "adding fragment to cache failed; ignoring error; cleaning up blob"
           >>= fun () ->
           let full_path = canonical path in
           Lwt.catch
             (fun ()  -> Lwt_unix.unlink full_path)
             (fun ex -> Lwt_log.warning_f ~section ~exn:ex "%s: cleanup failed" full_path
             )
          )
      in
      Alba_statistics.Statistics.with_timing_lwt
        (fun () ->
         Lwt_log.debug_f ~section "add %lx %S" bid oid >>= fun () ->
         Lwt_mutex.with_lock _mutex _add
        )
      >>= fun (t,()) ->
      Lwt_log.debug_f "add %lx %S took:%f" bid oid t


    method drop bid =
      Lwt_log.debug_f ~section "blob_cache # drop %li" bid >>= fun () ->
      Hashtbl.replace _dropping bid ();
      Lwt.return ()

    method close () =
      Lwt_log.warning_f ~section "closing database" >>= fun () ->
      Lwt_mutex.with_lock _mutex
      (fun () ->
       Rocks.RocksDb.close db;
       Lwt_extra2.with_fd
         db_path ~flags:[O_RDONLY] ~perm:0o644
         (fun fd ->
          Syncfs.lwt_syncfs fd  >>= fun _ ->
          Lwt.return ()
         )
       >>= fun () ->
       let fn = marker_name root in
       Lwt_extra2.with_fd fn
                          ~flags:[O_WRONLY;O_CREAT;O_EXCL]
                          ~perm:0o644
                          (fun fd -> Lwt.return ())
       >>= fun () ->
       Lwt_log.debug_f ~section "marker written:%s" fn >>= fun () ->
       Lwt.return ()
      )
end

let safe_create root ~max_size ~rocksdb_max_open_files =
  let fn = marker_name root in
  Lwt.catch
    (fun () ->
     Lwt_unix.unlink fn >>= fun () ->
     Lwt_log.debug_f "removed marker:%s" fn
    )
    (fun exn ->
     Lwt_log.info_f "couldn't remove marker, so removing everything" >>= fun () ->
     rm_tree ~silent:false (root ^ "/*" )
    )
  >>= fun () ->
  let cache = new blob_cache root ~max_size ~rocksdb_max_open_files in
  Lwt.return cache
