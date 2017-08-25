(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Lwt.Infix

open Nsm_model

class virtual cache = object(self)

    (* this method should _never_ throw! *)
    method virtual add :
                     int64 -> string
                     -> Bigstring_slice.t
                     -> (Manifest.t * int64 * string) list Lwt.t

    method add' bid oid blob =
      self # add bid oid blob >>= fun _ -> Lwt.return_unit

    method virtual lookup : timeout:float
                            -> int64 -> string
                            -> (SharedBuffer.t * (Manifest.t * int64 * string) list) option Lwt.t
    method virtual lookup2 : timeout:float
                             -> int64 -> string
                             -> (int * int * Lwt_bytes.t * int) list
                             -> (bool * (Manifest.t * int64 * string) list) Lwt.t

    method virtual drop  : int64 -> global : bool -> unit Lwt.t
    method virtual close : unit -> unit Lwt.t

    method virtual osd_infos : unit ->
                      (Albamgr_protocol.Protocol.alba_id *
                         ((Albamgr_protocol.Protocol.Osd.id * Nsm_model.OsdInfo.t *
                             Capabilities.OsdCapabilities.t))
                           counted_list)
                        counted_list Lwt.t
    method virtual has_local_fragment_cache : bool
    method virtual get_preset : alba_id:string -> namespace_id:int64 -> Preset.t option Lwt.t
end

class no_cache = object(self :#cache)
    inherit cache
    method add     bid oid blob   = Lwt.return []
    method lookup  ~timeout bid oid = Lwt.return_none
    method lookup2 ~timeout bid oid slices = Lwt.return (false, [])
    method drop    bid ~global    = Lwt.return_unit
    method close   ()             = Lwt.return_unit
    method osd_infos ()           = Lwt.return (0, [])
    method has_local_fragment_cache = false
    method get_preset ~alba_id ~namespace_id = Lwt.return_none
end

class x_cache (target: cache) =
  let make_key bid oid = Printf.sprintf "%Lx:%s" bid oid in
  let adding = Hashtbl.create 16 in
  let looking = Hashtbl.create 16 in

  object(self :# cache )
    method add bid oid blob  =
      let key = make_key bid oid in
      match Hashtbl.find adding key with
      | blob ->
         Lwt_log.debug_f "not adding %S" key >>= fun () ->
         Lwt.return []
      | exception Not_found ->
         begin
           let () = Hashtbl.add adding key blob in
           target # add bid oid blob >>= fun r ->
           let () = Hashtbl.remove adding key in
           Lwt.return r
         end

    method add' bid oid blob =
      self # add bid oid blob >>= fun _ ->
      Lwt.return_unit

    method lookup ~timeout bid oid =
      let key = make_key bid oid in
      match Hashtbl.find looking key with
      | (sleep, n) ->
         let () = incr n in
         Lwt_log.debug_f "x_cache: %S lookup already in progress" key
         >>= fun () ->
         sleep
      | exception Not_found ->
         begin
           let sleep, awake = Lwt.wait () in
           let () = Hashtbl.add looking key (sleep, ref 0) in
           target # lookup ~timeout bid oid
           >>= fun r ->
           match r with
           | None -> let () = Lwt.wakeup awake None in Lwt.return_none
           | Some (sb, mfs) ->
              begin
                let n = let _,nr = Hashtbl.find looking key in !nr in
                SharedBuffer.register_sharing ~n sb;
                Hashtbl.remove looking key;
                Lwt.wakeup awake r;
                Lwt.return r
              end
         end

    method lookup2 ~timeout bid oid slices =
      let key = make_key bid oid in
      match Hashtbl.find adding key with
      | blob ->
         Lwt_log.ign_debug_f "x_cache: %S : lookup2 during add" key;
         let () =
           List.iter
             (fun (offset, length, dest, dest_off) ->
               Bigstring_slice.blit blob offset length dest dest_off
             )
             slices
         in
         Lwt.return (true, [])
      | exception Not_found ->
         target # lookup2 ~timeout bid oid slices

    method drop bid ~global = target # drop bid ~global
    method close () = target # close ()
    method osd_infos () = target # osd_infos()
    method has_local_fragment_cache = target # has_local_fragment_cache
    method get_preset = target # get_preset
end
