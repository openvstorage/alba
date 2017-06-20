(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Checksum
open Slice

class type hasher =
object
  method reset : unit

  method update_string : string -> unit
  method update_substring : string -> int -> int -> unit
  method update_lwt_bytes : Lwt_bytes.t -> int -> int -> unit
  method update_lwt_bytes_detached : Lwt_bytes.t -> int -> int -> unit Lwt.t

  method final : unit -> Checksum.t
end


class no_hasher = (object
  method reset = ()

  method update_string (s:string) = ()
  method update_substring s pos len = ()
  method update_lwt_bytes bytes pos len = ()
  method update_lwt_bytes_detached bytes pos len = Lwt.return_unit

  method final () = Checksum.NoChecksum
end : hasher)

class sha1_hasher =
  let get_sha () =
    let open Gcrypt.Digest in
    let res = open_ SHA1 in
    Gc.finalise (fun t -> close t) res;
    res
  in
  (object(self)
    val mutable _sha = get_sha ()

    method reset = _sha <- get_sha ()

    method update_string (s:string) = Gcrypt.Digest.write _sha s
    method update_substring s pos len = Gcrypt.Digest.write_slice _sha (Slice.make s pos len)
    method update_lwt_bytes lwt_bytes pos len =
      Gcrypt.Digest.write_bs _sha ~release_runtime_lock:false (Bigstring_slice.from_bigstring lwt_bytes pos len)

    method update_lwt_bytes_detached s pos len =
      Lwt_preemptive.detach
        (Gcrypt.Digest.write_bs _sha ~release_runtime_lock:true)
        (Bigstring_slice.from_bigstring s pos len)

    method final () =
      let open Gcrypt.Digest in
      final _sha;
      let d = Option.get_some (read _sha SHA1) in
      Checksum.Sha1 d
  end : hasher)

class crc32c_hasher =
 (object(self)
      val mutable cur = 0xFFFFFFFFl

      method reset = cur <- 0xFFFFFFFFl

      method update_substring s pos len =
        cur <- Alba_crc32c.Crc32c.string ~crc:cur s pos len false

      method update_string s =
        self # update_substring s 0 (String.length s)

      method update_lwt_bytes lwt_bytes pos len =
        if len > 0
        then cur <- Alba_crc32c.Crc32c.big_array ~crc:cur lwt_bytes pos len false

      method update_lwt_bytes_detached s pos len =
        Lwt_preemptive.detach
          (fun () -> self # update_lwt_bytes s pos len)
          ()

      method final () =
        Checksum.Crc32c (Int32.logxor cur 0xFFFFFFFFl)

  end: hasher)

let make_hash : Checksum.algo -> hasher =
  let open Checksum.Algo in
  function
  | NO_CHECKSUM -> new no_hasher
  | SHA1        -> new sha1_hasher
  | CRC32c      -> new crc32c_hasher
