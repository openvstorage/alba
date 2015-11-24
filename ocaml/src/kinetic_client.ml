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

open Osd
open Lwt
open Kinetic
open Prelude
open Slice

let slice2s x_s = Slice.get_string_unsafe x_s
let s2slice x = Slice.wrap_string x

let build_forced = function
  | false -> None
  | true  -> Some true

let checksum2version cs =
  let b = Buffer.create 21 in
  let () = Checksum.Checksum.output b cs in
  let vs = Buffer.contents b in
  Some vs

let value2checksum v_s =
  let algo = Checksum.Checksum.Algo.CRC32c in
  let hasher = Hashes.make_hash algo in
  let () =
    let open Slice in
    hasher # update_substring
           v_s.buf
           v_s.offset
           v_s.length
  in
  hasher # final ()

let value2version v_s = value2checksum v_s |> checksum2version

type kinetic_update =
  | KSet of    (Kinetic.entry * bool)
  | KDelete of (Kinetic.entry * bool)

let show =
  let e2s = Kinetic.entry_to_string in function
  | KSet (e,b)   -> Printf.sprintf "KSet(%s,%b)" (e2s e) b
  | KDelete(e,b) -> Printf.sprintf "KDelete(%s,%b)" (e2s e) b

let forced_delete key =
  let entry = Kinetic.make_entry
                ~key ~db_version:None
                ~new_version:None None
  in
  KDelete(entry,true)

let forced_set_without_version key v_s =
  let new_version = None in
  let v = slice2s v_s in
  let tag = Kinetic.make_sha1 v in
  let vt = Some (v,tag) in
  let entry = Kinetic.make_entry
                ~key ~db_version:None
                ~new_version vt
  in
  KSet(entry, true)

let forced_set_with_version key v_s  =
  let new_version = value2version v_s in
  let v = slice2s v_s in
  let t = Kinetic.make_sha1 v in
  let vt = Some (v,t) in
  let entry = Kinetic.make_entry
                ~key ~db_version:None
                ~new_version vt
  in
  KSet(entry,true)

let conditional_delete key db_version =
  let entry = Kinetic.make_entry ~key ~db_version ~new_version:None None in
  KDelete(entry, false)

let conditional_set key db_version v_s cs =
  let new_version = value2version v_s in
  let v = slice2s v_s in
  let t = Kinetic.make_sha1 v in
  let vt = Some (v,t) in
  let entry = Kinetic.make_entry ~key ~db_version ~new_version vt in
  KSet(entry, false)

let translate alba_asserts alba_updates =
  begin
    let asserts =
      List.fold_left
        (fun asserts a->
         let Assert.Value(key_s, vo) = a in
         let key = slice2s key_s in
         StringMap.add key vo asserts
        )
        StringMap.empty alba_asserts
    in
    let set_keys =
      List.fold_left
        (fun sets u ->
         let Update.Set(key_s, _) = u in
         let key = slice2s key_s in
         StringSet.add key sets
        )
        StringSet.empty alba_updates
    in
    let fix_for_dangling_asserts asserts kseq =
      StringMap.fold
        (fun (k:string) a acc ->
         if StringSet.mem k set_keys
         then acc
         else
           let extra =
               match a with
               | None -> failwith "not supported"
               | Some v ->
                  (* TODO these copies should be avoided *)
                  let v_s = Asd_protocol.Blob.get_slice_unsafe v in
                  let cs = value2checksum v_s in
                  let db_version = checksum2version cs in
                  conditional_set k db_version v_s cs
           in
           extra:: acc
        ) asserts kseq
    in
    let translate_one = function
        | Update.Set(k,vco) ->
           let key = slice2s k in
           if not (StringMap.mem key asserts)
           then (* No asserts, so rather trivial *)
             match vco with
             | None          -> forced_delete key
             | Some (v,cs,assertable) ->
                (* TODO these copies should be avoided *)
                let v_s = Asd_protocol.Blob.get_slice_unsafe v in
                if assertable
                then forced_set_with_version key v_s
                else forced_set_without_version key v_s
           else
             begin
               (* asserts *)
               begin
                 match StringMap.find key asserts, vco with
                 | None          , None               ->
                    conditional_delete key None
                 | None          , Some (v,cs,_ )   ->
                    (* TODO these copies should be avoided *)
                    let v_s = Asd_protocol.Blob.get_slice_unsafe v in
                    conditional_set key None v_s cs
                 | (Some ov), None               ->
                    (* TODO these copies should be avoided *)
                    let ov_s = Asd_protocol.Blob.get_slice_unsafe ov in
                    conditional_delete key (value2version ov_s)
                 | (Some ov), Some (nv,ncs, asbl) ->
                    (* TODO these copies should be avoided *)
                    let ov_s = Asd_protocol.Blob.get_slice_unsafe ov in
                    let nv_s = Asd_protocol.Blob.get_slice_unsafe nv in
                    conditional_set key (value2version ov_s) nv_s ncs
               end
             end
    in
      let r0 = List.map translate_one alba_updates in
      fix_for_dangling_asserts asserts r0
  end


class kinetic_client cid session conn =

  object (self :# Osd.osd)

    method get_option prio key_s =
      (* TODO use prio *)
      let key = slice2s key_s in
      Lwt.catch
        (fun () ->
         Lwt_log.debug_f "get_option ~key:%S" key
         >>= fun () ->
         Kinetic.get session conn key >>= fun vco ->
         Lwt_log.debug_f "get_option result">>= fun () ->
         Lwt.return vco
        )
        (fun exn ->
         Lwt_io.printlf "get_option failed:%s%!" (Printexc.to_string exn)
         >>= fun () ->
         Lwt.fail exn
        )
      >>= fun vco ->
      match vco with
      | None -> Lwt.return None
      | Some (v, version) ->
         let vo_s = Some (Bigstring_slice.wrap_bigstring (Lwt_bytes.of_string v)) in
         Lwt.return vo_s


    method get_exn prio key_s =
      let key = slice2s key_s in
      Lwt_log.debug_f "kinetic_client.get_exn %S" key >>= fun () ->
      self # get_option prio key_s >>= function
      | None ->
         let f =  Failure
              ( Printf.sprintf "Could not find key:%S on .." key )
         in
         Lwt.fail f
      | Some x -> Lwt.return x

    method multi_get prio keys =
      (* TODO the semantics here are different from
         those for the asd *)
      Lwt_list.map_s
        (fun key -> self # get_option prio key)
        keys

    method multi_exists prio keys = failwith "not implemented"

    method range prio ~first ~finc ~last ~reverse ~max =
      (* TODO use prio *)
      (* TODO this can't handle max > 200 *)
      let first' = slice2s first in
      let last',linc = match last with
        | None -> String.make 10 '\xff' , false
        | Some (l,linc) -> slice2s l, linc
      in
      Kinetic.get_key_range
        session conn
        first' finc
        last' linc
        reverse max >>= fun keys ->
      let key_ss = List.map s2slice keys in
      (* TODO this is subawesome *)
      Lwt.return ((List.length key_ss, key_ss), key_ss <> [])


    method range_entries prio ~first ~finc ~last ~reverse ~max =
      Lwt.fail_with "TODO"

    method get_version = Lwt.fail_with "TODO"

    method apply_sequence prio asserts updates =
      (* TODO use prio *)
      let kseq = translate asserts updates in
      Lwt_log.debug_f "KINETIC:%s kseq:[%s]"
                      cid
                      (String.concat ";" (List.map show kseq))
      >>= fun () ->
      if kseq = []
      then Lwt.return Osd.Ok
      else
        begin
          let exn = ref [] in

          let waiters = ref [] in

          let get_handler () =
            let w, u = Lwt.wait () in
            waiters := w :: !waiters;
            fun rc ->
              Lwt_log.debug_f "KINETIC:%s my_handler" cid >>= fun () ->
              let rcc = Kinetic.convert_rc rc in
              (match rcc with
               | None -> Lwt.return ()
               | Some (i,m) ->
                 Lwt_log.info_f "KINETIC:%s my_handler: i=%i m=%S" cid i m
                 >>= fun () ->
                 let () = exn := (i,m) :: !exn in
                 Lwt.return ()) >>= fun () ->
              Lwt.wakeup u ();
              Lwt.return ()
          in

          Kinetic.start_batch_operation ~handler:(get_handler ()) session conn
          >>= fun batch ->

          Lwt_log.debug_f "KINETIC:%s starting batch: (%Li,%li) "
            cid
            (Kinetic.get_connection_id session)
            (Kinetic.get_batch_id batch) >>= fun () ->

          Lwt_list.iter_s
            (function
              | KSet(e, f)    ->
                 let forced = build_forced f in
                 Kinetic.batch_put batch e ~forced
              | KDelete(e, f) ->
                 let forced = build_forced f in
                 Kinetic.batch_delete batch e ~forced
            ) kseq
          >>= fun () ->
          Lwt_log.debug_f "KINETIC:%s ending batch (%Li, %li)"
                          cid
                          (Kinetic.get_connection_id session)
                          (Kinetic.get_batch_id batch)
          >>= fun () ->

          Kinetic.end_batch_operation batch ~handler:(get_handler ()) >>= fun conn' ->

          Lwt.join !waiters >>= fun () ->

          match !exn with
          | [] -> Lwt.return Ok
          | ims ->
             begin
               let ims' = List.rev ims in
               let e = Kinetic.Kinetic_exc ims' in
               Lwt_log.info_f
                 ~exn:e "KINETIC:%s [%s] connection_id:%Li batch_id:%li propagating"
                 cid
                 (String.concat ";" (List.map (fun (i,m) ->
                                               Printf.sprintf "(%i,%S)" i m )
                                              ims'
                 ))
                 (Kinetic.get_connection_id session)
                 (Kinetic.get_batch_id batch)
               >>= fun () ->
               Lwt.fail e (* TODO:
                             this should be interpreted and
                             maybe translated into an OSD.Exn
                           *)
             end
        end

    method set_full full =
      Lwt.fail_with "Kinetic_client: `set_full` is not implemented"
    method get_long_id = cid

    method get_disk_usage =
      (* TODO real implementation *)
      Lwt.return (Int64.shift_left 1L 41, Int64.shift_left 1L 42)
  end

let _client_id = ref 0

let make_client buffer_pool ips port (kinetic_id:string) =
  let cid = Printf.sprintf "(%03i, [%s], %i)"
                           !_client_id
                           (String.concat ";" ips)
                           port
  in
  let () = incr _client_id in
  Networking2.first_connection'
    buffer_pool
    ips port
    ~close_msg:"closing kinetic client"
  >>= fun (fd, conn, closer) ->
  Lwt.catch
    (fun () ->
       let secret = "asdfasdf" in
       let cluster_version = 0L in
       Kinetic.handshake secret cluster_version conn (*~trace:true*)
       >>= fun session ->
       let client = new kinetic_client cid session conn in
       Lwt.return (client, closer))
    (fun exn ->
       closer () >>= fun () ->
       Lwt.fail exn)

let with_client buffer_pool ips port kinetic_id f =
  make_client buffer_pool ips port kinetic_id
  >>= fun (c,closer) ->
  Lwt.finalize
    (fun () -> f c)
    (fun () -> closer ())
