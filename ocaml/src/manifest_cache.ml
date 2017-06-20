(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Lwt.Infix
open Cache

module ManifestCache = struct

  let deflate m =
    serialize
      ~buf_size:512
      Nsm_model.Manifest.inner_to_buffer_2 m

  let inflate deflated =
    deserialize
      Nsm_model.Manifest.inner_from_buffer_2
      deflated

    type cache_stat = {
        mutable hit: int;
        mutable miss: int;
      }[@@deriving show]

    type epoch = int

    type ('a,'b) t = {
        cache : (int64 * epoch * string, 'b) ACache.t;
        namespace_epoch : (int64, epoch) Hashtbl.t;
        mutable next_epoch : epoch;
        stats : (int64, cache_stat) Hashtbl.t;
      }




    let make (max_size:int)
      =
      { cache = ACache.make ~weight:String.length ~max_size (0L, 0, "");
        namespace_epoch = Hashtbl.create 3;
        next_epoch = 0;
        stats = Hashtbl.create 7;
      }

    let _find_stat t namespace_id =
      try Hashtbl.find t.stats namespace_id
      with Not_found ->
        let n = { hit = 0; miss = 0} in
        let () = Hashtbl.add t.stats namespace_id n in
        n

    let _hit t namespace_id =
      let stat = _find_stat t namespace_id in
      stat.hit <- stat.hit + 1;
      stat

    let _miss t namespace_id =
      let stat = _find_stat t namespace_id in
      stat.miss <- stat.miss + 1;
      stat

    let get_epoch t namespace_id =
      try Hashtbl.find t.namespace_epoch namespace_id with
      | Not_found ->
        let epoch = t.next_epoch in
        Hashtbl.add t.namespace_epoch namespace_id epoch;
        t.next_epoch <- epoch + 1;
        epoch

    let add t namespace_id object_name a =
      Lwt_log.ign_debug_f "add %Li %S to manifest cache" namespace_id object_name;

      let epoch = get_epoch t namespace_id in
      let b = deflate a in
      Lwt_log.ign_debug_f "deflated value is %i bytes" (Bytes.length b);

      ACache.add t.cache (namespace_id, epoch, object_name) b

    let remove t namespace_id object_name =
      let epoch = get_epoch t namespace_id in
      ACache.remove t.cache (namespace_id, epoch, object_name)

    let lookup_multiple t
                        namespace_id object_names
                        lookup_slow
                        ~consistent_read
                        ~should_cache
      =
      if consistent_read
      then
        begin
          lookup_slow object_names >>= fun manifests ->
          let r =
            List.map2
              (if should_cache
               then
                 fun object_name ->
                 function
                 | None -> (Stale, None)
                 | Some manifest ->
                    add t namespace_id object_name manifest;
                    (Stale, Some manifest)
               else
                 fun _ manifest -> (Stale, manifest))
              object_names
              manifests
          in
          Lwt.return r
        end
      else
        begin
          let epoch = get_epoch t namespace_id in
          Lwt_list.map_p
            (fun object_name ->
              match ACache.lookup t.cache (namespace_id, epoch, object_name) with
              | None ->
                 let () = _miss t namespace_id |> ignore in
                 begin
                   lookup_slow [ object_name; ] >>= function
                   | [ Some manifest; ] ->
                      add t namespace_id object_name manifest;
                      Lwt.return (Slow, Some manifest)
                   | [ None; ] ->
                      Lwt.return (Slow, None)
                   | _ -> assert false
                 end
              | Some mf_s ->
                 let () = _hit t namespace_id |> ignore in
                 let r = Some (inflate mf_s) in
                 Lwt.return (Fast, r))
            object_names
        end

    let _invalidate t namespace_id =
      Hashtbl.remove t.namespace_epoch namespace_id;
      let stat = _find_stat t namespace_id in
      stat.hit <- 0;
      stat.miss <- 0

    let invalidate t namespace_id =
      _invalidate t namespace_id;
      Lwt.return ()

    let drop t namespace_id =
      (* the current design is too simple to support this *)
      _invalidate t namespace_id
end
