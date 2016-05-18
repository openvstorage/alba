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

type config = {
    write_blobs: bool;
    files_path: string;
    use_fadvise: bool;
    use_fallocate: bool;
  }

type directory_status =
  | Exists
  | Creating of unit Lwt.t

let get_file_name fnr = Printf.sprintf "%016Lx" fnr

type fnr = int64

class virtual directory_access =
object
  method virtual delete_dir : bytes -> unit Lwt.t

  method virtual ensure_dir_exists : bytes -> sync : bool -> unit Lwt.t
end

class default_directory_access files_path =
object(self)
  inherit directory_access
            
  val directory_cache : (string, directory_status) Hashtbl.t = Hashtbl.create 3

  initializer
    begin
      let p = files_path in
      if p.[0] <> '/'
      then failwith (Printf.sprintf "'%s' should be an absolute path" p);
      Hashtbl.add directory_cache "." Exists
    end
  
  method ensure_dir_exists dir ~sync =
    Lwt_log.debug_f "ensure_dir_exists: %s" dir >>= fun () ->
    match Hashtbl.find directory_cache dir with
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
           Hashtbl.add directory_cache dir (Creating sleep);

           let parent_dir = Filename.dirname dir in
           self # ensure_dir_exists parent_dir ~sync >>= fun () ->

           Lwt_extra2.create_dir
             ~sync
             (Filename.concat files_path dir))
        (function
          | Unix.Unix_error (Unix.EEXIST, _, _) ->
            Lwt.return ()
          | exn ->
            Hashtbl.remove directory_cache dir;
            (* need to wake up the waiter here so it doesn't wait forever *)
            Lwt.wakeup_exn awake exn;
            Lwt.fail exn) >>= fun () ->

      Hashtbl.replace directory_cache dir Exists;
      Lwt.wakeup awake ();
      Lwt.return_unit
        
  method delete_dir = function
    | "."
    | "" -> Lwt.fail_with "just don't"
    | dir ->
      begin
        let full_dir = Filename.concat files_path dir in
        match Hashtbl.find directory_cache dir with
        | Exists ->
           Hashtbl.remove directory_cache dir;
           Lwt_unix.rmdir full_dir
        | Creating wait -> Lwt.fail_with (Printf.sprintf "creating %s" dir)
        | exception Not_found ->
                    Lwt_unix.rmdir full_dir
      end
        
end
  
class virtual blob_access =
object
  method virtual config : config

  method virtual get_blob : fnr -> int -> bytes Lwt.t

  method virtual with_blob_fd : fnr -> (Lwt_unix.file_descr -> unit Lwt.t) -> unit Lwt.t

  method virtual write_blob : fnr
                              -> Asd_protocol.Blob.t
                              -> post_write: Asd_io_scheduler.post_write
                              -> sync_parent_dirs:bool
                              -> unit Lwt.t

  method virtual delete_blobs : fnr list -> ignore_unlink_error:bool -> unit Lwt.t

  (* TODO: These should go *)
  method virtual _get_file_dir_name_path : fnr -> bytes * bytes * bytes
  method virtual _get_file_path : fnr -> bytes
end
                       
class directory_info config =
object(self)
  inherit blob_access
  inherit default_directory_access config.files_path
            
  method config = config  

  method _get_file_dir_name_path fnr =
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
        [ config.files_path; dir; file ] in
    dir, file, path

  method _get_file_path fnr =
    let _, _, path = self # _get_file_dir_name_path fnr in
    path

  method with_blob_fd fnr f =
    Lwt_extra2.with_fd
      (if config.write_blobs
       then self # _get_file_path fnr
       else "/dev/zero")
      ~flags:Lwt_unix.([O_RDONLY;])
      ~perm:0600
      f

  method get_blob fnr size =
    Lwt_log.debug_f "getting blob %Li with size %i" fnr size >>= fun () ->
    let bs = Bytes.create size in
    self # with_blob_fd
      fnr
      (fun fd ->
         Lwt_extra2.read_all fd bs 0 size >>= fun got ->
         assert (got = size);
         Lwt.return ()) >>= fun () ->
    Lwt_log.debug_f "got blob %Li" fnr >>= fun () ->
    Lwt.return bs

  method write_blob
           (fnr:int64)
           (blob: Asd_protocol.Blob.t)
           ~(post_write:Asd_io_scheduler.post_write) 
           ~(sync_parent_dirs:bool)
    =
    Lwt_log.debug_f "writing blob %Li (use_fadvise:%b use_fallocate:%b)"
                    fnr config.use_fadvise config.use_fallocate
    >>= fun () ->
    with_timing_lwt
      (fun () ->
         let dir, _, file_path = self # _get_file_dir_name_path fnr in
         self # ensure_dir_exists dir ~sync:sync_parent_dirs >>= fun () ->
         Lwt_extra2.with_fd
           file_path
           ~flags:Lwt_unix.([ O_WRONLY; O_CREAT; O_EXCL; ])
           ~perm:0o664
           (fun fd ->
             let open Osd in
             let len = Blob.length blob in
             (if config.use_fallocate
              then
                Posix.lwt_fallocate fd 0 0 len
              else
                Lwt.return_unit
             ) >>= fun () ->
             let open Asd_protocol.Blob in
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
                 let open Slice.Slice in
                 Lwt_extra2.write_all
                   fd
                   s.buf s.offset len
             )
             >>= fun () ->
             let parent_dir = config.files_path ^ "/" ^ dir in
             post_write fd len parent_dir
             >>= fun () ->
             let ufd = Lwt_unix.unix_file_descr fd in
             let () =
               if config.use_fadvise
               then Posix.posix_fadvise ufd 0 len Posix.POSIX_FADV_DONTNEED
             in
             Lwt.return_unit
           )
      )
    >>= fun (t_write, ()) ->

    (if t_write > 0.5
     then Lwt_log.info_f
     else Lwt_log.debug_f)
      "written blob %Li, took %f" fnr t_write

  method delete_blobs fnrs ~ignore_unlink_error =
    
    (* TODO bulk sync of (unique) parent filedescriptors *)
    let fnrs = List.sort Int64.compare fnrs in
    Lwt_list.iter_s
      (fun fnr ->
        let path = self # _get_file_path fnr in
        Lwt_extra2.unlink
          ~may_not_exist:(ignore_unlink_error || not config.write_blobs)
          ~fsync_parent_dir:true
          path
      )
      fnrs
    
end


    
