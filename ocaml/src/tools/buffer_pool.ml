open Prelude

module Buffers = WeakHashSet(struct
                                type t = Lwt_bytes.t
                                let hash = Hashtbl.hash
                                let equal = ( == )
                              end)

type t = { bs : Buffers.t;
           buffer_size : int; }

let create ~buffer_size =
  { bs = Buffers.create 3;
    buffer_size; }

let get_buffer t = match Buffers.pop t.bs with
  | Some b -> b
  | None -> Lwt_bytes.create t.buffer_size

let return_buffer t b = Buffers.add t.bs b
