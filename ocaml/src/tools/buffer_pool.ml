type t = Lwt_bytes.t Weak_pool.t

let create ~buffer_size =
  Weak_pool.create (fun () -> Lwt_bytes.create buffer_size)

let get_buffer = Weak_pool.take

let return_buffer = Weak_pool.return
