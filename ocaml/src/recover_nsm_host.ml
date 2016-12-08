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
open Slice
open Rocks_store
open Lwt_buffer
open Recovery_info

(*
====================

next exercise:
recover albamgr when all nsm hosts are known and alive

====================

final exercise:
recover albamgr + nsm hosts

 *)


(*

scenario: lost an nsm host, but we still have the albamgr

- list namespaces on this nsm host from albamgr
- list osds for those namespaces

- register each of those namespaces on an nsm host with status rebuilding

- scan all osds for recovery info in all of these namespaces

- for each fragment: add info about it to nsm
  - index incomplete objects? -> yes, so we can repair them (if we got enough fragments), and so we/ops can know all went according to plan (there are no more incomplete objs at the end of the procedure)
- periodically register osd scanning progress to nsm

- determine highest seen gc epoch (simple range query on osds)

- when all done:
  - verify/repair all objects that are incomplete

- put status back to normal and allow uploads again
  (or allow uploads immediately...)

 *)

(*

hmm process opsplitsen in stappen:
- bepaal namespaces to be recovered, print list
- allow ops to register new destination for the namespace
- allow kicking off recovery for this namespace

 *)

(*

need some tests for this...

 *)

type fragment_info = {
    object_id : string;
    chunk_id : int;
    fragment_id : int;
    version_id : int;
    recovery_info : RecoveryInfo.t';
    osd_id : int64;
  } [@@ deriving show]


module Keys = struct
  let ns_info = "ns_info"
  let total_workers = "total_workers"
  let worker_id = "worker_id"
  let osd_status ~osd_id = "osd_status" ^ (serialize x_int64_to osd_id)
  let fragments_prefix = 'f'

  let fragment_key ~object_id ~chunk_id ~fragment_id ~version_id=
    serialize
      (Llio.tuple5_to
         Llio.char_to
         Llio.string_to
         Llio.int_to
         Llio.int_to
         Llio.int_to)
      (fragments_prefix, object_id, chunk_id, fragment_id, version_id)

  let parse_fragment_key key =
    deserialize
      (Llio.tuple5_from
         Llio.char_from
         Llio.string_from
         Llio.int_from
         Llio.int_from
         Llio.int_from)
      key

end

module R = Rocks_key_value_store
module K = Osd_keys.AlbaInstance

let wo =
  let open Rocks in
  let r = WriteOptions.create_no_gc () in
  WriteOptions.set_sync r false;
  r

let reap_osd
    (alba_client : Alba_client.alba_client)
    kv
    ~osd_id
    ~namespace_id
    total_workers worker_id =

  let boundary = function
    | i when i = total_workers ->
      K.fragment_recovery_info_next_prefix
    | i ->
      let b = i * (1 lsl 32) / total_workers in
      let buf = Buffer.create 32 in
      x_int64_be_to buf (Int64.of_int b);
      Buffer.add_bytes buf (Bytes.make 28 '\000');
      let bs = Buffer.contents buf in

      K.fragment_recovery_info
        ~object_id:bs
        ~version_id:0
        ~chunk_id:0 ~fragment_id:0
  in

  let osd_status_key = Keys.osd_status ~osd_id in

  let end_object_id = boundary (worker_id + 1) in

  let rec inner first finc =
    let rec get_recovery_info delay =
      Lwt.catch
        (fun () ->
           alba_client # with_osd
             ~osd_id
             (fun osd ->
                (osd # namespace_kvs namespace_id) # range_entries
                  Osd.High
                  ~first ~finc
                  ~last:(Some (Slice.wrap_string end_object_id, false))
                  ~reverse:false ~max:1000))
        (fun exn ->
           Lwt_log.info_f ~exn "Exception while getting keys from osd %Li" osd_id >>= fun () ->
           Lwt_unix.sleep delay >>= fun () ->
           get_recovery_info (delay *. 1.5))
    in

    get_recovery_info 1. >>= fun ((_, keys), has_more) ->

    if keys = []
    then Lwt.return ()
    else begin

      let last_key, _, _ = List.last_exn keys in

      let open Rocks in
      WriteBatch.with_t
        (fun wb ->
           List.iter
             (fun (k, v, cs) ->
                let object_id, chunk_id, fragment_id, version_id =
                  K.parse_fragment_recovery_info (Slice.get_string_unsafe k) in
                (* TODO use cs to verify v *)
                let recovery_info =
                  deserialize
                    RecoveryInfo.from_buffer
                    (Lwt_bytes.to_string v) in

                WriteBatch.put_string
                  wb
                  (Keys.fragment_key ~object_id ~chunk_id ~fragment_id ~version_id)
                  (serialize
                     (Llio.pair_to
                       RecoveryInfo.to_buffer
                       x_int64_to)
                    (recovery_info, osd_id)))
             keys;

           WriteBatch.put_string
             wb
             osd_status_key
             (Slice.get_string_unsafe last_key);

           Rocks.write kv ~opts:wo wb;
        );

      if has_more
      then inner last_key false
      else Lwt.return ()
    end
  in

  let first, finc =
    match R.get kv osd_status_key with
    | None -> boundary worker_id, true
    | Some v -> v, false
  in

  inner (Slice.wrap_string first) finc


let gather_and_push_objects
    (alba_client : Alba_client.alba_client)
    ~namespace_id
    ~encryption
    kv =

  let open Rocks in
  let ro = ReadOptions.create () in
  let it = Iterator.create kv ~opts:ro in

  let buf : fragment_info list Lwt_buffer.t = Lwt_buffer.create () in

  Lwt.ignore_result begin
    let inner () =
      Lwt_buffer.take buf >>= fun acc ->
      Lwt_log.debug_f "Took item from buf, trying to recover it..."
      >>= fun () ->

      let sort_version infos =
        List.sort
          (fun i0 i1 -> i1.version_id - i0.version_id)
          infos
      in
      let fs =
        acc |>
        (List.group_by
           (fun { chunk_id; _; } -> chunk_id)) |>
        Hashtbl.to_assoc_list |>
        List.sort (fun (c1, _) (c2, _) -> compare c1 c2) |>
        List.map
          (fun (chunk_id, fs) ->
             let fs =
               List.group_by
                 (fun { fragment_id; _; } -> fragment_id)
                 fs |>
               Hashtbl.to_assoc_list
             in
             let find_best (fid,(infos : fragment_info list))=
               let infos_sorted = sort_version infos in
               List.hd_exn infos_sorted
             in
             chunk_id,
             List.map
               (* only keep one fragment for each chunk_id,
                  fragment_id combination *)
               find_best
               fs)
      in
      let last_chunk_id, last_chunk = List.last_exn fs in
      let object_id, recovery_info_last =
        let x = List.hd_exn last_chunk in
        x.object_id, x.recovery_info
      in

      let object_name = recovery_info_last.RecoveryInfo.name in
      (match recovery_info_last.RecoveryInfo.object_info_o with
       | None ->
          (* did not yet see enough chunks (and thus also fragment)
           for this object. the last chunk should have a bit more
           info in it's recovery info... *)
          Lwt.fail_with "not enough chunks yet!"
       | Some o -> Lwt.return o)
      >>= fun { RecoveryInfo.storage_scheme;
                timestamp;
                size; checksum; } ->

      assert (List.length fs = last_chunk_id + 1);
      let fs_reduced = (* use information in chunk's
                          fragment with highest version *)
        List.map
          (fun (chunk_id, chunk_fragments) ->
            let sorted = sort_version chunk_fragments in
            (chunk_id, List.hd_exn sorted)
          )
        fs
      in
      let chunk_sizes =
        List.map
          (fun (_, {recovery_info; } ) -> recovery_info.RecoveryInfo.chunk_size)
          fs_reduced
      in

      let fragment_checksums =
        List.map
          (fun (_, {recovery_info;} ) ->
           recovery_info.RecoveryInfo.fragment_checksums)
          fs_reduced
      in

      let fragment_packed_sizes =
        List.map
          (fun (_, {recovery_info;}) ->
            recovery_info.RecoveryInfo.fragment_sizes)
          fs_reduced
      in

      let open Nsm_model in
      let open RecoveryInfo in
      let Storage_scheme.EncodeCompressEncrypt (ec, compression) =
        (recovery_info_last.object_info_o
         |> Option.get_some).storage_scheme in
      let Encoding_scheme.RSVM (k, m, w) = ec in

      let fragment_locations =
        List.map
          (fun (_, chunk_fragments) ->
           let cnt = ref 0 in
           let res =
             List.mapi
               (fun fragment_id _ ->
                match List.find
                        (fun f1 -> f1.fragment_id = fragment_id)
                        chunk_fragments with
                | None -> None, 0
                | Some f ->
                   incr cnt;
                   Some f.osd_id, f.version_id)
               (Int.range 0 (k+m))
           in
           (* only do upload when there are enough fragments
              fragments each chunk!
              (repair by policy can take it from there)
            *)
           assert (!cnt >= k);
           res)
          fs
      in

      let version_id =
        List.fold_left
          (fun max_v (_, fs) ->
           List.fold_left
             (fun max_v f  ->
              max max_v f.version_id)
             max_v
             fs)
          0
          fs
      in

      let fragment_ctrs =
        Layout.unfold
          ~n_chunks:(List.length fs)
          ~n_fragments:(k + m)
          (fun chunk_id fragment_id ->
            match List.find_exn (fun (chunk_id', _) -> chunk_id' = chunk_id) fs
                  |> snd
                  |> List.find (fun f1 -> f1.fragment_id = fragment_id) with
            | None -> None
            | Some f -> f.recovery_info.fragment_ctr
          )
      in

      let open Nsm_model in
      let manifest : Manifest.t =
        Manifest.make
          ~name:object_name
          ~object_id
          ~storage_scheme
          ~size
          ~checksum
          ~encrypt_info:(Encrypt_info_helper.from_encryption encryption)
          ~timestamp
          ~chunk_sizes
          ~fragment_locations
          ~fragment_checksums
          ~fragment_packed_sizes
          ~version_id
          ~fragment_ctrs
          (* TODO *)
          ~max_disks_per_node:100
      in

      Lwt_log.debug_f
        "Storing manifest into nsm: %s"
        (Manifest.show manifest) >>= fun () ->

      alba_client # nsm_host_access # get_nsm_by_id ~namespace_id
      >>= fun nsm ->
      nsm # put_object
          ~allow_overwrite:NoPrevious
          ~gc_epoch:0L
          ~manifest >>= fun _ ->

      Lwt.return ()
    in
    inner ()
  end;

  Iterator.seek_string it (String.make 1 Keys.fragments_prefix);

  (* gather fragments by object id and push the
     result on a buffer (which is processed by a
     bunch of other fibers)
  *)
  let rec inner acc =
    if Iterator.is_valid it
    then begin
      let k = Iterator.get_key_string it in
      if k.[0] = Keys.fragments_prefix
      then begin
        let _, object_id, chunk_id, fragment_id, version_id =
          Keys.parse_fragment_key k in
        let recovery_info', osd_id =
          deserialize
            (Llio.pair_from
               RecoveryInfo.from_buffer
               x_int64_from)
            (Iterator.get_value_string it) in
        RecoveryInfo.t_to_t'
          recovery_info'
          encryption
          ~object_id >>= fun recovery_info ->

        let cur = { object_id;
                    chunk_id;
                    fragment_id;
                    version_id;
                    recovery_info;
                    osd_id;
                  } in
        Iterator.next it;
        match acc with
        | [] ->
          inner [cur]
        | it :: tl ->
          if it.object_id = object_id
          then begin
            (* another fragment of the same object, keep on accumulating *)
            let acc' = cur :: acc in
            inner acc'
          end else
            (* fragment for another object, pass off current accumulator
               to someone else to handle it *)
            (* op queue die duwen die limited is in size
               hoop andere threads werken da queueke af
            *)
            Lwt_log.debug_f "adding item to buf 1" >>= fun () ->
            Lwt_buffer.add acc buf >>= fun () ->
            inner [cur]
      end
      else
        Lwt_log.debug_f "adding item to buf 2" >>= fun () ->
        Lwt_buffer.add acc buf
    end
    else
      Lwt_log.debug_f "adding item to buf 3" >>= fun () ->
      Lwt_buffer.add acc buf
  in
  inner [] >>= fun () ->
  Lwt_buffer.wait_until_empty buf >>= fun () ->
  (* TODO properly wachten tot laatste object in nsm geschoten is *)
  Lwt_unix.sleep 1.


let nsm_recovery_agent
    (alba_client : Alba_client.alba_client)
    namespace
    home
    total_workers
    worker_id
    osds
  =

  let kv = R.create' ~db_path:home () in

  let open Albamgr_protocol.Protocol in

  (match R.get kv Keys.ns_info with
   | Some v ->
     let namespace', ns_info =
       deserialize
          (Llio.pair_from
             Llio.string_from
             Namespace.from_buffer)
          v
     in
     assert (namespace' = namespace);
     Lwt_log.info_f "Found namespace info in rocksdb" >>= fun () ->
     Lwt.return ns_info
   | None ->
     Lwt_log.info_f "Fetching namespace info from albamgr" >>= fun () ->
     alba_client # mgr_access # list_namespaces
       ~first:namespace
       ~finc:true
       ~last:(Some (namespace, true))
       ~max:1
       ~reverse:false >>= fun ((_, namespaces), _) ->
     let _, ns_info = List.hd_exn namespaces in
     R.set
       kv
       Keys.ns_info
       (serialize
         (Llio.pair_to
            Llio.string_to
            Namespace.to_buffer)
         (namespace, ns_info));
     Lwt.return ns_info) >>= fun ns_info ->
  let namespace_id = ns_info.Namespace.id in

  (* check/set workers + worker_id *)
  (match R.get kv Keys.total_workers with
   | Some v ->
     let total_workers' = deserialize Llio.int_from v in
     assert (total_workers = total_workers')
   | None ->
     R.set kv Keys.total_workers (serialize Llio.int_to total_workers));
  (match R.get kv Keys.worker_id with
   | Some v ->
     let worker_id' = deserialize Llio.int_from v in
     assert (worker_id = worker_id')
   | None ->
     R.set kv Keys.worker_id (serialize Llio.int_to worker_id));

  (* start threads to scrape an osd *)
  Lwt_log.info "Start reap osd" >>= fun () ->
  Lwt_list.map_p
    (fun osd_id ->
       reap_osd
         alba_client kv
         ~osd_id ~namespace_id
         total_workers worker_id)
    osds >>= fun (_ : unit list) ->

  Lwt_log.info "gather_and_push_objects" >>= fun () ->
  gather_and_push_objects
    alba_client
    ~namespace_id
    ~encryption:Encryption.Encryption.NoEncryption
    kv
