open Core_kernel
type t = {
  bs : Bigstring.t;
  offset : int;
  length : int;
}

let from_bigstring bs offset length =
  { bs; offset; length; }

let wrap_bigstring bs =
  from_bigstring bs 0 (Lwt_bytes.length bs)

let create length =
  wrap_bigstring (Bigstring.create length)

let ptr_start t =
  let open Ctypes in
  bigarray_start array1 t.bs +@ t.offset

let extract_to_bigstring t =
  Lwt_bytes.extract t.bs t.offset t.length

let extract t offset length =
  let bs = Lwt_bytes.extract t.bs (t.offset + offset) length in
  wrap_bigstring bs

let length t = t.length
