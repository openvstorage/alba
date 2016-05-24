module GMemPool : sig
  type t
  val init  : int -> unit
  val alloc : int -> t
  val free  : t -> unit
end

module Fragment : sig
  type t
  val show : t -> string
  type completion_id = int64
  val make : completion_id -> int -> int -> GMemPool.t -> t
  val get_bytes : t -> Lwt_bytes.t
  val get_completion_id : t -> completion_id
  val get_offset : t -> int
  val get_size : t -> int

  val free_bytes : t -> unit
end

module Batch : sig
  type t
  val make : Fragment.t list -> t
  val show : t -> string
end

module Ser : sig
  val get64_prim' : Lwt_bytes.t -> int -> int64
  val get32_prim' : Lwt_bytes.t -> int -> int32

end

module IOExecFile : sig
  val init : string -> unit
  val destroy: unit -> unit


  type handle
  val show_handle: handle -> string

  type fd = Lwt_unix.file_descr
  val file_open: string -> Unix.open_flag list -> handle Lwt.t
  val file_write: handle -> Batch.t -> unit Lwt.t
  val file_read: handle -> Batch.t -> unit Lwt.t
  val file_close: handle -> unit Lwt.t
  val file_delete: string -> Fragment.completion_id -> unit Lwt.t
  val get_event_fd : unit -> fd

  type status
  val show_status : status -> string
  val get_completion_id : status -> Fragment.completion_id
  val get_error_code : status -> int32

  val reap : fd -> Lwt_bytes.t -> (int * status list) Lwt.t
end

(* (* we only want this when packaged distributed as a package *)
module Version : sig
  val major : int
  val minor : int
  val patch : int
  val git_revision : string
  val summary : int * int * int * string
end
 *)
