(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

open Prelude
open Lwt.Infix
open Cache

module ManifestCache = struct

    let deflate m =
      serialize
        ~buf_size:512
        Nsm_model.Manifest.to_buffer m

    let inflate deflated =
      deserialize
        Nsm_model.Manifest.from_buffer
        deflated

    type cache_stat = {
        mutable hit: int;
        mutable miss: int;
      }[@@deriving show]

    type epoch = int

    type ('a,'b) t = {
        cache : (int32 * epoch * string, 'b) ACache.t;
        namespace_epoch : (int32, epoch) Hashtbl.t;
        mutable next_epoch : epoch;
        stats : (int32, cache_stat) Hashtbl.t;
      }


    let make (max_size:int)
      =
      { cache = ACache.make ~weight:String.length ~max_size (0l, 0, "");
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
      Lwt_log.ign_debug_f "add %li %S to manifest cache" namespace_id object_name;

      let epoch = get_epoch t namespace_id in
      let b = deflate a in
      Lwt_log.ign_debug_f "deflated value is %i bytes" (Bytes.length b);

      ACache.add t.cache (namespace_id, epoch, object_name) b

    let remove t namespace_id object_name =
      let epoch = get_epoch t namespace_id in
      ACache.remove t.cache (namespace_id, epoch, object_name)

    let lookup t
               namespace_id object_name
               lookup_slow
               ~consistent_read
               ~should_cache
      =
      Lwt_log.debug_f "lookup %li %S ~consistent_read:%b ~should_cache:%b"
                      namespace_id
                      object_name
                      consistent_read should_cache
      >>= fun () ->
      let epoch = get_epoch t namespace_id in
      let slow_path namespace_id object_name =
        lookup_slow namespace_id object_name >>= function
        | None    -> Lwt.return None
        | (Some mf) as mo ->
           begin
             if should_cache
             then add t namespace_id object_name mf;

             Lwt.return mo
           end
      in
      if not consistent_read
      then
        match ACache.lookup t.cache (namespace_id, epoch, object_name) with
        | None ->
           let _hm = _miss t namespace_id in
           Lwt_log.debug_f
             "slow_path for namespace_id:%li object_name:%S"
             namespace_id object_name
           >>= fun () ->
           slow_path namespace_id object_name >>= fun r ->
           Lwt.return (false, r)
        | Some deflated ->
           let _hm = _hit t namespace_id in
           let r = Some (inflate deflated) in
           Lwt.return (true,r)
      else
        let _hm = _find_stat t namespace_id in
        slow_path namespace_id object_name >>= fun r ->
        Lwt.return (false, r)

    let invalidate t namespace_id =
      Hashtbl.remove t.namespace_epoch namespace_id;
      let stat = _find_stat t namespace_id in
      stat.hit <- 0;
      stat.miss <- 0;
      Lwt.return ()

    let drop t namespace_id =
      (* the current design is too simple to support this *)
      ()
end
