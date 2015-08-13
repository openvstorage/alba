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
    osd_id : int32;
  }

module Keys = struct
  let ns_info = "ns_info"
  let total_workers = "total_workers"
  let worker_id = "worker_id"
  let osd_status ~osd_id = "osd_status" ^ (serialize Llio.int32_to osd_id)
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
      K.fragment_recovery_info_next_prefix ~namespace_id
    | i ->
      let b = i * (1 lsl 32) / total_workers in
      let buf = Buffer.create 32 in
      Llio.int32_be_to buf (Int32.of_int b);
      Buffer.add_bytes buf (Bytes.make 28 '\000');
      let bs = Buffer.contents buf in

      K.fragment_recovery_info
        ~namespace_id
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
                osd # range_entries
                  ~first ~finc
                  ~last:(Some (Slice.wrap_string end_object_id, false))
                  ~reverse:false ~max:1000))
        (fun exn ->
           Lwt_log.info_f ~exn "Exception while getting keys from osd %li" osd_id >>= fun () ->
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
                let _, object_id, chunk_id, fragment_id, version_id =
                  K.parse_fragment_recovery_info (Slice.get_string_unsafe k) in
                (* TODO use cs to verify v *)
                let recovery_info =
                  deserialize
                    RecoveryInfo.from_buffer
                    (Slice.get_string_unsafe v) in

                WriteBatch.put
                  wb
                  (Keys.fragment_key ~object_id ~chunk_id ~fragment_id ~version_id)
                  (serialize
                     (Llio.pair_to
                       RecoveryInfo.to_buffer
                       Llio.int32_to)
                    (recovery_info, osd_id)))
             keys;

           WriteBatch.put
             wb
             osd_status_key
             (Slice.get_string_unsafe last_key);

           RocksDb.write kv wo wb;
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
  let ro = ReadOptions.create_gc () in
  let it = Iterator.create kv ro in

  let buf : fragment_info list Lwt_buffer.t = Lwt_buffer.create () in

  Lwt.ignore_result begin
    let inner () =
      Lwt_buffer.take buf >>= fun acc ->
      Lwt_log.debug_f "Took item from buf, trying to recover it..." >>= fun () ->

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
             chunk_id,
             List.map
               (* only keep one fragment for each chunk_id, fragment_id combination *)
               (compose List.hd_exn snd)
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

      let chunk_sizes =
        List.map
          (fun (_, chunk_fragments) ->
           let { recovery_info; } = List.hd_exn chunk_fragments in
           recovery_info.RecoveryInfo.chunk_size)
          fs
      in

      let fragment_checksums =
        List.map
          (fun (_, chunk_fragments) ->
           let { recovery_info; } = List.hd_exn chunk_fragments in
           recovery_info.RecoveryInfo.fragment_checksums)
          fs
      in

      let fragment_packed_sizes =
        List.map
          (fun (_, chunk_fragments) ->
           let { recovery_info; } = List.hd_exn chunk_fragments in
           recovery_info.RecoveryInfo.fragment_sizes)
          fs
      in

      let fragment_locations =
        List.mapi
          (fun chunk_id l ->
           let _, chunk_fragments = List.nth_exn fs chunk_id in
           List.mapi
             (fun fragment_id _ ->
              match List.find
                      (fun f1 -> f1.fragment_id = fragment_id)
                      chunk_fragments with
              | None -> None, 0
              | Some f -> Some f.osd_id, f.version_id)
             l)
          fragment_packed_sizes
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

      (* TODO only do this when there are enough fragments
         for each chunk!
         (repair by policy can take it from there)
       *)

      let open Nsm_model in
      let manifest : Manifest.t =
        Manifest.make
          ~name:object_name
          ~object_id
          ~storage_scheme
          ~size
          ~checksum
          ~encrypt_info:(EncryptInfo.from_encryption encryption)
          ~timestamp
          ~chunk_sizes
          ~fragment_locations
          ~fragment_checksums
          ~fragment_packed_sizes
          ~version_id

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

  Iterator.seek it (String.make 1 Keys.fragments_prefix);

  (* gather fragments by object id and push the
     result on a buffer (which is processed by a
     bunch of other fibers)
  *)
  let rec inner acc =
    if Iterator.is_valid it
    then begin
      let k = Iterator.get_key it in
      if k.[0] = Keys.fragments_prefix
      then begin
        let _, object_id, chunk_id, fragment_id, version_id =
          Keys.parse_fragment_key k in
        let recovery_info', osd_id =
          deserialize
            (Llio.pair_from
               RecoveryInfo.from_buffer
               Llio.int32_from)
            (Iterator.get_value it) in
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
    worker_id =

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

  Lwt_log.info_f "Fetching list of namespace osds ..." >>= fun () ->
  alba_client # mgr_access # list_all_claimed_osds >>= fun (_, osds) ->

  (* start threads to scrape an osd *)
  Lwt_list.map_p
    (fun (osd_id, _) ->
       reap_osd
         alba_client kv
         ~osd_id ~namespace_id
         total_workers worker_id)
    osds >>= fun (_ : unit list) ->

  gather_and_push_objects
    alba_client
    ~namespace_id
    ~encryption:Encryption.Encryption.NoEncryption
    kv
